import pytest
from mictlanx.utils.index import Utils

def test_split_path_01():
    x = Utils.split_path(path="/source/test_x.pdf")
    assert x[0] == "/source"
    assert x[1] == "test_x"
    assert x[2] == "pdf"

def test_split_path_02():
    x = Utils.split_path(path="/source/test",is_file=False)
    assert x[0] == "/source/test"
    assert x[1] == ""
    assert x[2] == ""

def test_split_path_03():
    x = Utils.split_path(path="/source/test")
    assert x[0] == "/source"
    assert x[1] == "test"
    assert x[2] == ""