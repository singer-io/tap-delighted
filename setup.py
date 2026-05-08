from setuptools import find_packages, setup

setup(name="tap-delighted",
      version="0.1.0",
      description="Singer.io tap for extracting data from delighted API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_delighted"],
      install_requires=[
        "singer-python==6.8.0",
        "requests==2.33.0",
        "backoff==2.2.1",
        "parameterized"
      ],
      entry_points="""
          [console_scripts]
          tap-delighted=tap_delighted:main
      """,
      packages=find_packages(),
      package_data={
          "tap_delighted": ["schemas/*.json"],
      },
      include_package_data=True,
)
