def pytest_addoption(parser):
    parser.addoption(
        "--bucketid",
        action="store",
        default="b1",
        help="Custom bucket id parameter for tests"
    )

    parser.addoption(
        "--key",
        action="store",
        default="x",
        help="Custom key parameter for tests"
    )