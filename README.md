# Nexus.Sources.WindCube

This data source extension makes it possible to read data files in the WindCube format into Nexus.

To use it, put a `config.json` with the following sample content into the database root folder:

```json
{
  "/A/B/C": {
    "FileSourceGroups": [
      {
        "Name": "default",
        "PathSegments": [
          "'DATA'",
          "yyyy-MM"
        ],
        "FileTemplate": "'abc_'yyyy_MM_dd__HH_mm_ss'.sta'",
        "FilePeriod": "1.00:00:00",
        "UtcOffset": "00:10:00"
      }
    ]
  }
}
```

Please see the [tests](tests/Nexus.Sources.WindCube.Tests) folder for a complete sample.