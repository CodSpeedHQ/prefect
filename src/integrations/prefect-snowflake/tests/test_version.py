from packaging.version import Version


def test_version():
    from prefect_snowflake import __version__

    assert isinstance(__version__, str)
    assert Version(__version__)
    assert __version__.startswith("0.")
