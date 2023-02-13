# Copyright (c) 2016 Shotgun Software Inc.
#
# CONFIDENTIAL AND PROPRIETARY
#
# This work is provided "AS IS" and subject to the Shotgun Pipeline Toolkit
# Source Code License included in this distribution package. See LICENSE.
# By accessing, using, copying or modifying this work you indicate your
# agreement to the Shotgun Pipeline Toolkit Source Code License. All rights
# not expressly granted therein are reserved by Shotgun Software Inc.
import gc

from .data_handler import ShotgunDataHandler
from .errors import ShotgunModelDataError
from .data_handler_cache import ShotgunDataHandlerCache
from .util import compare_shotgun_data
import sgtk
from collections import defaultdict
import os


class ShotgunFindDataHandler(ShotgunDataHandler):
    """
    Shotgun Model low level data storage for use
    with the Shotgun Model.

    This implements a data storage where a single
    shotgun find query is stored in the cache file.
    """

    def __init__(
        self,
        entity_type,
        filters,
        order,
        hierarchy,
        fields,
        download_thumbs,
        limit,
        additional_filter_presets,
        cache_path,
    ):
        """
        :param entity_type:               Shotgun entity type to download
        :param filters:                   List of Shotgun filters. Standard Shotgun syntax.
        :param order:                     Order clause for the Shotgun data. Standard Shotgun API syntax.
        :param hierarchy:                 List of grouping fields. These should be names of Shotgun
                                          fields. If you for example want to create a list of items,
                                          the value ``["code"]`` will be suitable. This will generate a data
                                          model which is flat and where each item's default name is the
                                          Shotgun name field. If you want to generate a tree where assets
                                          are broken down by asset type, you could instead specify
                                          ``["sg_asset_type", "code"]``.
        :param fields:                    List of field names to retrieve from Shotgun (in addition to
                                          the ones specified in the hierarchy parameter).
        :param download_thumbs:           Boolean to indicate if this model should attempt
                                          to download and process thumbnails for the downloaded data.
        :param limit:                     Limit the number of results returned from Shotgun. In conjunction with the
                                          order
                                          parameter, this can be used to effectively cap the data set that the model
                                          is handling, allowing a user to for example show the twenty most recent note
                                          or similar.
        :param additional_filter_presets: List of Shotgun filter presets to apply, e.g.
                                          ``[{"preset_name":"LATEST","latest_by":"BY_PIPELINE_STEP_NUMBER_AND_ENTITIES_CREATED_AT"}]``
        :param cache_path:                Path to cache file location
        """
        super(ShotgunFindDataHandler, self).__init__(cache_path)
        self.__entity_type = entity_type
        self.__filters = filters
        self.__order = order
        self.__hierarchy = hierarchy
        self.__fields = fields
        self.__download_thumbs = download_thumbs
        self.__limit = limit
        self.__additional_filter_presets = additional_filter_presets
        self._sg_data = None
        self._p4 = None
        self._peforce_data = {}

    def get_entity_ids(self):
        """
        Returns a list of entity ids contained in this data set given an entity type.

        Performs a sequential scan over all items in the data set.

        :return: A list of unique ids for all items in the model.
        :rtype: ``list``
        """
        if not self.is_cache_loaded():
            return []

        # loop over all cache items - the find data handler organizes
        # its unique ids so that all leaf nodes (e.g. representing an entity)
        # are ints and all other items are strings
        entity_ids = []
        for data_item in self._cache.get_all_items():
            if data_item.is_leaf():
                entity_ids.append(data_item.unique_id)

        return entity_ids

    def get_uid_from_entity_id(self, entity_id):
        """
        Returns the unique id for a given entity or None if not found.

        :param entity_id: Shotgun entity id to resolve
        :returns: unique id as string or int, to be used with
                  :meth:`get_data_item_from_uid`.
        """
        if not self.is_cache_loaded():
            return None

        # leaf nodes are keyed by entity id
        if self._cache.item_exists(entity_id):
            return entity_id
        else:
            return None

    def generate_data_request(self, data_retriever):
        """
        Generate a data request for a data retriever.

        Once the data has arrived, the caller is expected to
        call meth:`update_data` and pass in the received
        data payload for processing.

        :param data_retriever: :class:`~tk-framework-shotgunutils:shotgun_data.ShotgunDataRetriever` instance.
        :returns: Request id or None if no work is needed
        """
        # only request data from shotgun is filters are defined.
        if self.__filters is None:
            return None

        # get data from shotgun - list/set cast to ensure unique fields
        fields = self.__hierarchy + self.__fields
        if self.__download_thumbs:
            fields = fields + ["image"]
        fields = list(set(fields))

        find_kwargs = dict(limit=self.__limit)

        # We only want to include the filter presets kwarg if it was explicitly asked
        # for. The reason for this is that it's a Shotgun 7.0 feature server side, and
        # we don't want to break backwards compatibility with older versions of Shotgun.
        if self.__additional_filter_presets:
            find_kwargs["additional_filter_presets"] = self.__additional_filter_presets

        request_id = data_retriever.execute_find(
            self.__entity_type, self.__filters, fields, self.__order, **find_kwargs
        )

        return request_id

    @sgtk.LogManager.log_timing
    def update_data(self, sg_data):
        """
        The counterpart to :meth:`generate_data_request`. When the data
        request has been carried out, this method should be called by the calling
        class and the data payload from Shotgun should be provided via the
        sg_data parameter.

        The shotgun find data is compared against the existing tree and
        a list of differences is returned, indicating which nodes were
        added, deleted and modified, on the following form::

            [
             {
                "data": ShotgunItemData instance,
                "mode": self.UPDATED|ADDED|DELETED
             },
             {
                "data": ShotgunItemData instance,
                "mode": self.UPDATED|ADDED|DELETED
             },
             ...
            ]

        :param sg_data: list, resulting from a Shotgun find query
        :returns: list of updates. see above
        :raises: :class:`ShotgunModelDataError` if no cache is loaded into memory
        """
        self._log_debug("Updating %s with %s shotgun records." % (self, len(sg_data)))
        self._log_debug("Hierarchy: %s" % self.__hierarchy)

        new_cache = ShotgunDataHandlerCache()

        # If we don't already have a cache, we can continue on here by just populating
        # our new cache with the necessary data.
        if self._cache is None:
            self.load_cache()

        if self._cache.size == 0:
            self._log_debug("In-memory cache is empty.")

        # ensure the data is clean
        self._log_debug("sanitizing data...")
        sg_data = self._sg_clean_data(sg_data)
        self._log_debug("...done!")

        self._connect()
        # Get perforce data
        sg_data = self._get_peforce_data(sg_data)
        #self._get_latest_revision(files_to_sync)

        # self._log_debug(">>>>>>> sg_data is: {}".format(sg_data))
        self._log_debug("Generating new tree in memory...")

        # create a brand new tree rather than trying to be clever
        # about how we cull intermediate nodes for deleted items
        diff_list = []
        num_adds = 0
        num_deletes = 0
        num_modifications = 0

        # analyze the incoming shotgun data
        if not sg_data:
            return None
        else:
            for sg_item in sg_data:

                parent_uid = None

                # Create items by drilling down the hierarchy
                for field_name in self.__hierarchy:

                    on_leaf_level = self.__hierarchy[-1] == field_name

                    if not on_leaf_level:
                        # generate path for this item
                        unique_field_value = self.__generate_unique_key(
                            parent_uid, field_name, sg_item
                        )
                    else:
                        # on the leaf level, use the entity id as the unique key
                        unique_field_value = sg_item["id"]

                    # two distinct cases for leaves and non-leaves
                    if on_leaf_level:
                        # this is an actual entity - insert into our new tree
                        new_cache.add_item(
                            parent_uid, sg_item, field_name, True, unique_field_value
                        )

                        # now check with prev data structure to see if it has changed
                        if not self._cache.item_exists(unique_field_value):
                            # this is a new node that wasn't there before
                            diff_list.append(
                                {
                                    "data": new_cache.get_entry_by_uid(unique_field_value),
                                    "mode": self.ADDED,
                                }
                            )
                            num_adds += 1
                        else:
                            # record already existed in prev dataset. Check if value has changed
                            old_record = self._cache.get_shotgun_data(unique_field_value)
                            if not compare_shotgun_data(old_record, sg_item):
                                diff_list.append(
                                    {
                                        "data": new_cache.get_entry_by_uid(
                                            unique_field_value
                                        ),
                                        "mode": self.UPDATED,
                                    }
                                )
                                num_modifications += 1

                    else:
                        # not on leaf level yet
                        if not new_cache.item_exists(unique_field_value):
                            # item is not yet inserted in our new tree so add it
                            # because these are parent items like project nodes
                            new_cache.add_item(
                                parent_uid, sg_item, field_name, False, unique_field_value
                            )

                            # now check with prev data structure to see if it has changed
                            if not self._cache.item_exists(unique_field_value):
                                # this is a new node that wasn't there before
                                diff_list.append(
                                    {
                                        "data": new_cache.get_entry_by_uid(
                                            unique_field_value
                                        ),
                                        "mode": self.ADDED,
                                    }
                                )
                                num_adds += 1
                            else:
                                # record already existed in prev dataset. Check if value has changed
                                current_record = self._cache.get_shotgun_data(
                                    unique_field_value
                                )
                                # don't compare the whole record but just the part that relates to this
                                # intermediate node value. For example, we may be looking at a project node
                                # in the hierarchy but the full sg record contains all the data for a shot.
                                # in this case, just run the comparison on the project subset of the full
                                # shot data dict.
                                if not compare_shotgun_data(
                                    current_record.get(field_name), sg_item.get(field_name)
                                ):
                                    diff_list.append(
                                        {
                                            "data": self._cache.get_entry_by_uid(
                                                unique_field_value
                                            ),
                                            "mode": self.UPDATED,
                                        }
                                    )
                                    num_modifications += 1

                        # recurse down to the next level
                        parent_uid = unique_field_value

            # now figure out if anything has been removed
            self._log_debug("Diffing new tree against old tree...")

            current_uids = set(self._cache.uids)
            new_uids = set(new_cache.uids)

            for deleted_uid in current_uids.difference(new_uids):
                diff_list.append(
                    {
                        "data": self._cache.get_entry_by_uid(deleted_uid),
                        "mode": self.DELETED,
                    }
                )
                num_deletes += 1

            # Lastly, swap in the new cache
            self._cache = None

            # at this point, kick the gc to make sure the memory is freed up
            # despite its cycles.
            gc.collect()

            # and set the new cache
            self._cache = new_cache
            """
            self._log_debug(
                ">>>>>>>>>>>>>>>>>>>>>>>>>> ShotGrid data:"
            )
            if sg_data:
                for i, item_dict in enumerate(sg_data):
                    self._log_debug(
                        ">>>>>>>>>>> {}".format(i)
                    )
                    for k, v in item_dict.items():
                        self._log_debug(
                            ">>>>> {}:{}".format(k, v)
                        )
            """
            self._log_debug(
                "ShotGrid data (%d records) received and processed. " % len(sg_data)
            )
            self._log_debug("    The new tree is %d records." % self._cache.size)
            self._log_debug(
                "    There were %d diffs from in-memory cache:" % len(diff_list)
            )
            self._log_debug("    Number of new records: %d" % num_adds)
            self._log_debug("    Number of deleted records: %d" % num_deletes)
            self._log_debug("    Number of modified records: %d" % num_modifications)

        # self._sg_data = sg_data
        return diff_list

    def _get_peforce_data(self, sg_data):
        if sg_data:
            if len(sg_data) <= 20:
                sg_data = self._get_small_peforce_data(sg_data)
            else:
                sg_data = self._get_large_peforce_data(sg_data)
        return sg_data

    def _get_large_peforce_data(self, sg_data):
        """"
        Get large perforce data
        """
        item_path_dict = defaultdict(int)
        fstat_dict = {}
        if sg_data:
            for i, sg_item in enumerate(sg_data):
                if "path" in sg_item:
                    if "local_path" in sg_item["path"]:
                        local_path = sg_item["path"].get("local_path", None)
                        # self._log_debug("local_path is: {}".format(local_path))
                        item_path = self._get_item_path(local_path)
                        # self._log_debug("item_path is: {}".format(item_path))
                        if item_path not in item_path_dict:
                            item_path_dict[item_path] = 1
                        else:
                            item_path_dict[item_path] += 1
            # self._log_debug(">>>>>>>>>>  item_path_dict is: {}".format(item_path_dict))

            for key in item_path_dict:
                if key:
                    # self._log_debug(">>>>>>>>>>  key is: {}".format(key))
                    key = "{}\\...".format(key)
                    self._log_debug("^^^ key is: {}".format(key))
                    fstat_list = self._p4.run("fstat", key)
                    for i, fstat in enumerate(fstat_list):
                        # if i == 0:
                        #    self._log_debug(">>>>>>>>>>  fstat is: {}".format(fstat))
                        client_file = fstat.get('clientFile', None)
                        # if i == 0:
                        #    self._log_debug(">>>>>>>>>>  client_file is: {}".format(client_file))
                        if client_file:
                            have_rev = fstat.get('haveRev', "0")
                            head_rev = fstat.get('headRev', "0")
                            # if i == 0:
                            #    self._log_debug(">>>>>>>>>>  have_rev is: {}".format(have_rev))
                            #    self._log_debug(">>>>>>>>>>  head_rev is: {}".format(head_rev))
                            modified_client_file = self._create_key(client_file)
                            if modified_client_file not in fstat_dict:

                                # if i == 0:
                                #    self._log_debug(">>>>>>>>>>  client_file is: {}".format(client_file))
                                fstat_dict[modified_client_file] = {}
                                fstat_dict[modified_client_file]['clientFile'] = client_file
                                fstat_dict[modified_client_file]['haveRev'] = have_rev
                                fstat_dict[modified_client_file]['headRev'] = head_rev
                                # if i == 0:
                                #     self._log_debug(">>>>>>>>>>  fstat_dict[client_file] is: {}".format(fstat_dict[modified_client_file]))
            # self._log_debug(">>>>>>>>>>  fstat_dict is: {}".format(fstat_dict))

            for i, sg_item in enumerate(sg_data):
                if "path" in sg_item:
                    if "local_path" in sg_item["path"]:
                        local_path = sg_item["path"].get("local_path", None)
                        modified_local_path = self._create_key(local_path)

                        # if i == 0:
                        # self._log_debug(">>>>>>>>>>  sg_item is: {}".format(sg_item))
                        # self._log_debug(">>>>>>>>>>  sg_item_path is: {}".format(sg_item["path"]))
                        # self._log_debug(">>>>>>>>>>  local_path is: {}".format(local_path))
                        # self._log_debug(">>>>>>>>>>  modified_local_path is: {}".format(modified_local_path))
                        # if modified_local_path in fstat_dict:
                        #    self._log_debug(">>>>>>>>>>  modified_local_path in fstat_dict is: {}".format(fstat_dict[modified_local_path]))

                        if modified_local_path and modified_local_path in fstat_dict:

                            have_rev = fstat_dict[modified_local_path].get('haveRev', "0")
                            head_rev = fstat_dict[modified_local_path].get('headRev', "0")
                            # self._log_debug(">>>>>>>>>>  have_rev is: {}".format(have_rev))
                            # self._log_debug(">>>>>>>>>>  head_rev is: {}".format(head_rev))

                            sg_item["haveRev"], sg_item["headRev"] = have_rev, head_rev
                            sg_item["revision"] = "{}/{}".format(have_rev, head_rev)
                # if i == 0:
                #    self._log_debug(">>> {}: SG item: {}".format(i, sg_item))

        return sg_data

    def _create_key(self, file_path):
        if file_path:
            file_path = file_path.replace("\\", "")
            file_path = file_path.replace("/", "")
            file_path = file_path.lower()
        return file_path


    def _get_item_path (self, local_path):
        """
        Get item path
        """
        item_path = ""
        if local_path:
            local_path = local_path.split("\\")
            local_path = local_path[:7]
            item_path = "\\".join(local_path)
        return item_path

    def _get_small_peforce_data(self, sg_data):
        """"
        Get small perforce data
        """
        
        if sg_data:
            for i, sg_item in enumerate(sg_data):
                if "path" in sg_item:
                    if "local_path" in sg_item["path"]:
                        local_path = sg_item["path"].get("local_path", None)
                        # self._log_debug(">>>>>>> local_path is: {}".format(local_path))
                        if local_path:
                            fstat_list = self._p4.run("fstat", local_path)
                            # self._log_debug("fstat_list: {}".format(fstat_list))
                            fstat = fstat_list[0]
                            # self._log_debug("fstat is: {}".format(fstat))
                            have_rev = fstat.get('haveRev', "0")
                            head_rev = fstat.get('headRev', "0")
                            sg_item["haveRev"], sg_item["headRev"] = have_rev, head_rev
                            sg_item["revision"] = "{}/{}".format(have_rev, head_rev )
                            # self._log_debug("{}: Revision: {}".format(i, sg_item["revision"]))
                            # sg_item['depotFile'] = fstat.get('depotFile', None)

                # self._log_debug("{}: SG item: {}".format(i, sg_item))

        return sg_data

    def _get_latest_revision(self, files_to_sync):
        for file_path in files_to_sync:
            p4_result = self._p4.run("sync", "-f", file_path + "#head")
            self._log_debug("Syncing file: {}".format(file_path))

    def _to_sync (self, have_rev, head_rev):
        """
        Determine if we should sync the file
        """
        have_rev_int = int(have_rev)
        head_rev_int = int(head_rev)
        if head_rev_int > 0 and have_rev_int < head_rev_int:
            return True
        return False

    def _get_depot_path(self, local_path):
        """
        Convert local path to depot path
        For example, convert: 'B:\\Ark2Depot\\Content\\Base\\Characters\\Human\\Survivor\\Armor\\Cloth_T3\\_ven\\MDL\\Survivor_M_Armor_Cloth_T3_MDL.fbx'
        to "//Ark2Depot/Content/Base/Characters/Human/Survivor/Armor/Cloth_T3/_ven/MDL/Survivor_M_Armor_Cloth_T3_MDL.fbx"
        """
        local_path = local_path[2:]
        depot_path = local_path.replace("\\", "/")
        depot_path = "/{}".format(depot_path)
        return depot_path

    def _connect(self):
        """
        Connect to Perforce.  If a connection can't be established with
        the current settings then the connection UI will be shown.
        """
        try:
            if not self._p4:
                self._log_debug("Connecting to perforce ...")
                self._fw = sgtk.platform.get_framework("tk-framework-perforce")
                self._p4 = self._fw.connection.connect()

        except:
            #Todo add error message
            self._log_debug("Failed to connect!")
            raise

    def _get_data(self):
        return self._sg_data

    def __generate_unique_key(self, parent_unique_key, field, sg_data):
        """
        Generates a unique key from a shotgun field.

        Used in conjunction with the hierarchical nature of the find data handler.

        :param parent_unique_key: uid for the parent
        :param field: the shotgun field that the node represents
        :param sg_data: associated shotgun data dictionary
        :returns: Unique string or int
        """
        # note: these ids are written to disk and kept in memory
        # and thus affect memory usage and i/o peformance. We assume
        # that a vast majority of nodes are leaves and store these as
        # ints for compactness. non-leaf nodes have a "path-like" string
        # to uniquely describe their location in the tree.
        #
        # we assume that on each level, values are unique.

        value = sg_data.get(field)

        if isinstance(value, dict) and "id" in value and "type" in value:
            # for single entity links, return the entity id
            unique_key = "%s_%s" % (value["type"], value["id"])

        elif isinstance(value, list):
            # this is a list of some sort. Loop over all elements and extract a comma separated list.
            formatted_values = []
            for v in value:
                if isinstance(v, dict) and "id" in v and "type" in v:
                    # This is a link field
                    formatted_values.append("%s_%s" % (v["type"], v["id"]))
                else:
                    formatted_values.append(str(v))
            unique_key = ",".join(formatted_values)

        else:
            # everything else just cast to string
            unique_key = str(value)

        if parent_unique_key is None:
            # no parent
            return "/%s" % unique_key
        else:
            return "%s/%s" % (parent_unique_key, unique_key)
