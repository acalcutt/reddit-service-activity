from setuptools import setup, find_packages

# Prefer the refactored location for Baseplate's Thrift build command, then
# try the legacy integration path, and finally fall back to setuptools' build
# implementation so editable installs and metadata preparation don't fail.
try:
    from baseplate.frameworks.thrift.command import ThriftBuildPyCommand  # type: ignore
except Exception:
    try:
        from baseplate.integration.thrift.command import ThriftBuildPyCommand  # type: ignore
    except Exception:
        from setuptools.command.build_py import build_py as ThriftBuildPyCommand


setup(
    name="reddit_service_activity",
    packages=find_packages(exclude="tests"),
    install_requires=[
        "baseplate",
        "thrift",
        "pyramid",
        "redis",
    ],
    tests_require=[
        "nose",
        "coverage",
        "mock",
    ],
    cmdclass={
        "build_py": ThriftBuildPyCommand,
    },
)
