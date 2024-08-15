import os

from .basetest import BaseTest


class TestParser(BaseTest):
    def test_pp_converter(self):
        from mogwai.parser.powerpoint_converter import ppt_to_dic

        test_pp_file = os.path.join(self.documents_path, "test_pp1.pptx")
        dic = ppt_to_dic(test_pp_file)
        self.assertTrue(dic["author"] == "Musselman, Kristin", "wrong author")
        self.assertTrue(dic["slides"][0]["page"] == 1, "wrong first page number")
        print("test_pp_converter successfully")

    def test_xml_converter(self):
        from mogwai.parser.xml_converter import xml_file_to_dic

        test_xml_path = os.path.join(self.documents_path, "test_xml.xml")
        dic = xml_file_to_dic(test_xml_path)
        calorie = dic["menu"]["food"][0]["calories"]
        name = dic["menu"]["food"][3]["name"]
        self.assertEqual(calorie, "650", "wrong number of calories")
        self.assertEqual(name, "French Toast", "wron food name")
        print("test_xml_converter successfully")

    def test_excel_converter(self):
        from mogwai.parser.excel_converter import excel_to_dic

        test_excel_path = os.path.join(self.documents_path, "test_excel.xlsx")
        dic = excel_to_dic(test_excel_path)
        creator = dic["creator"]
        s1c1v1 = dic["sheets"][0]["column1"]["1"]
        self.assertEqual(creator, "Marcel Grün", "wrong creator")
        self.assertEqual(s1c1v1, "a3", "wrong cell value")
        print("test_excel_converter successfully")


if __name__ == "__main__":
    import unittest

    unittest.main()
