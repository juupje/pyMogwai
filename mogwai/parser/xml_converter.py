import xmltodict

def xml_file_to_dic(xml_path:str) -> dict:
    test_xml_file = open(xml_path, "r")
    dic = xml_str_to_dic(test_xml_file.read())
    test_xml_file.close()
    return dic

def xml_str_to_dic(xml:str) -> dict:
    return xmltodict.parse(xml)

if __name__ == "__main__":
    pass