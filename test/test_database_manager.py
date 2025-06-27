import unittest
import logging
import sys
import uuid

from uuid import uuid4
from typing import List

from assasdb import AssasDatabaseManager, AssasAstecArchive, AssasDocumentFileStatus

logger = logging.getLogger("assas_test")

logging.basicConfig(
    format="%(asctime)s %(module)s %(levelname)s: %(message)s",
    level=logging.DEBUG,
    stream=sys.stdout,
)


class SBO_fb_test_samples:
    def __init__(
        self,
        config: dict,
    ) -> None:
        self._config = config
        self._upload_uuids = [
            uuid.UUID("2bdd775d-442c-487f-a0a0-9aec7f47d796"),
            uuid.UUID("ce3b0594-c213-4339-a334-f4a099b17da9"),
        ]
        self._archive_list = [
            SBO_fb_test_samples.archive_factory(upload_uuid)
            for upload_uuid in self._upload_uuids
        ]

    def get_archive_list(self) -> List[AssasAstecArchive]:
        return self._archive_list

    @staticmethod
    def archive_factory(upload_uuid: uuid4) -> AssasAstecArchive:
        return AssasAstecArchive(
            upload_uuid=f"{str(upload_uuid)}",
            name="SBO fb",
            date="08/05/2024, 23:25:37",
            user="ke4920",
            description="Station blackout scenario number, with 2 parameters",
            archive_path=(
                f"/mnt/ASSAS/upload_test/{str(upload_uuid)}/STUDY/TRANSIENT/BASE_SIMPLIFIED"
                "/SBO/SBO_feedbleed/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin"
            ),
            result_path=f"/mnt/ASSAS/upload_test/{str(upload_uuid)}/result/dataset.h5",
        )


class AssasDatabaseManagerTest(unittest.TestCase):
    def setUp(self):
        self.database_manager = AssasDatabaseManager()

    def tearDown(self):
        self.database_manager = None

    def test_database_manager_empty(self):
        self.database_manager.empty_internal_database()

    def test_database_manager_get_datasets(self):
        frames = self.database_manager.get_all_database_entries()
        print(set(frames["system_status"]))

    """
    def test_database_manager_SBO_fb_100_samples_register(self):

        self.database_manager.empty_internal_database()

        archives = SBO_fb_test_samples(self.config).get_archive_list()
        logger.info(len(archives))

        self.database_manager.register_archives(archives)

        entries = self.database_manager.get_all_database_entries()
        self.assertEqual(len(entries), len(archives))

    def test_database_manager_process_upload(self):

        upload_uuid = uuid.UUID('e406d8aa-f370-4b58-9da1-3a896cd04a87')

        self.database_manager.process_upload(upload_uuid)

        document = self.database_manager.get_database_entry_by_upload_uuid(upload_uuid)

        self.assertEqual(str(upload_uuid), document['system_upload_uuid'])
    """

    def test_database_manager_process_uploads(self):
        self.assertTrue(self.database_manager.process_uploads())

    def test_database_manager_process_uploads_reload(self):
        self.assertTrue(self.database_manager.process_uploads_with_reload_flag())

    def test_database_manager_get_size(self):
        archive_path = "/mnt/ASSAS/upload_test/0c65e12b-a75b-486b-b3ff-cc68fc89b78a/"
        archive_path += "STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/"
        archive_path += "SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin"

        size_bytes = AssasDatabaseManager.get_size_of_directory_in_bytes(archive_path)
        size = AssasDatabaseManager.convert_from_bytes(size_bytes)
        print(f"size {size}")

    def test_database_manager_get_file_size(self):
        archive_path = (
            "/mnt/ASSAS/upload_test/0c65e12b-a75b-486b-b3ff-cc68fc89b78a/"
            "result/dataset.h5"
        )
        size_bytes = AssasDatabaseManager.get_file_size(archive_path)
        size = AssasDatabaseManager.convert_from_bytes(size_bytes)

        assert isinstance(size, str)
        assert len(size) > 0

    def test_database_manager_update_archive_sizes(self):
        self.assertTrue(
            self.database_manager.update_archive_sizes(number_of_archives=10)
        )

    def test_database_manager_set_status(self):
        document_uuid = uuid.UUID("77022ac7-8b43-48da-93c5-6ec999fd12ff")
        status = AssasDocumentFileStatus.INVALID

        self.database_manager.set_document_status_by_uuid(document_uuid, status)

    def test_database_manager_set_size(self):
        document_uuid = uuid.UUID("d4bc85ff-cbd2-4c41-bb60-5238356ecadb")
        size = "10.5 MB"

        self.database_manager.set_hdf5_size_by_uuid(document_uuid, size)

    def test_database_manager_update_upload_information(self):
        document_uuid = uuid.UUID("b6279ecb-0580-4ee9-862a-6754c62ff89c")
        archive_path = (
            "/all_samples/sample_1/STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/"
            "SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin"
        )
        archive_paths = [archive_path]

        self.database_manager.update_upload_info(
            document_uuid, "archive_paths", archive_paths
        )

    """
    def test_database_manager_set_to_converted(self):

        frames = self.database_manager.get_all_database_entries()
        print(set(frames['system_status']))

        for idx in frames.index:

            if frames['system_status'][idx] == AssasDocumentFileStatus.VALIDATED:

                status = AssasDocumentFileStatus.CONVERTED

                document_uuid_str = frames['system_uuid'][idx]
                document_uuid = uuid.UUID(document_uuid_str)

                self.database_manager.set_document_status_by_uuid(document_uuid, status)

    def test_database_manager_get_upload_uuids(self):

        AssasDatabaseManager.get_upload_uuids2('/mnt/ASSAS/upload_test')
    """

    def test_database_manager_convert_next_10_time_points(self):
        self.database_manager.convert_next_validated_archive(explicit_times=[0, 10])

    def test_database_manager_convert_next(self):
        self.database_manager.convert_next_validated_archive()

    def test_database_manager_collect_meta(self):
        self.database_manager.update_meta_data_of_valid_archives()

    def test_database_manager_get_upload_time(self):
        directory = "/mnt/ASSAS/upload_test/defb5a82-edeb-4efb-b824-fd15d95317cf"
        self.database_manager.get_upload_time(directory)

    def test_database_manager_backup_internal_database(self):
        self.database_manager.backup_internal_database()

    def test_database_manager_reset_invalid_archives(self):
        self.database_manager.reset_invalid_archives()

    def test_database_manager_reset_converting_archives(self):
        self.database_manager.reset_converting_archives()

    def test_database_manager_reset_valid_archives(self):
        self.database_manager.reset_valid_archives()

    def test_database_manager_reset_all_result_files(self):
        self.database_manager.reset_all_result_files()

    def test_database_manager_update_meta_data(self):
        document_uuid = uuid.UUID("144f6875-b09e-45d5-9656-0cfbac61c7ab")
        self.database_manager.update_meta_data(uuid=document_uuid)

    def test_database_manager_reset_result_file(self):
        system_uuid = uuid.UUID("9cf84cfd-2dd0-456c-9593-b7c9ff337fed")
        self.database_manager.reset_result_file_by_uuid(system_uuid=system_uuid)

    def test_database_manager_update_upload_info(self):
        upload_uuid = uuid.UUID("c11bbcdd-78b9-481b-a8cb-dbffa4c6af94")
        self.database_manager.update_upload_info(
            upload_uuid=upload_uuid,
            key="archive_paths",
            value_list=["/Sample_144/LOCA_6I_CL_1300_LIKE_SIMPLIFIED_ASSAS_FILT.bin"],
        )

    def test_database_manager_read_entries_from_backup(self):
        dataframe = self.database_manager.get_all_database_entries_from_backup()

        assas_archive_meta = dataframe.loc[0]

        upload_uuid = assas_archive_meta["system_upload_uuid"]
        input_path = assas_archive_meta["system_path"]
        output_path = assas_archive_meta["system_result"]

        logger.info(f"upload_uuid: {str(upload_uuid)}")
        logger.info(f"input_path: {str(input_path)}")
        logger.info(f"output_path: {str(output_path)}")

    def test_database_manager_get_overall_database_size(self):
        size = self.database_manager.get_overall_database_size()
        logger.info(f"overall size: {size}")
        self.assertIsInstance(size, str)
        self.assertTrue(len(size) > 0)

    def test_database_manager_collect_number_of_samples(self):
        self.database_manager.collect_number_of_samples_of_uploaded_archives()


if __name__ == "__main__":
    unittest.main()
