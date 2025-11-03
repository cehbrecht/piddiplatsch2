# README Handle/PIDs

## stac schema

GeoJSON spec
https://datatracker.ietf.org/doc/html/rfc7946#section-3

STAC Item spec
https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md

STAC Common Metadata
https://github.com/radiantearth/stac-spec/blob/master/commons/common-metadata.md

STAC Assets
https://github.com/radiantearth/stac-spec/blob/master/commons/assets.md

CMIP6 Extension
https://github.com/stac-extensions/cmip6

File Extension
https://github.com/stac-extensions/file



## esgf docs

esgf docu:
* https://github.com/ESGF/esgf-roadmap/blob/main/status/20250701-ESGF-NG.md#data-challenges

stac schema doc:
https://docs.google.com/document/d/1O7CsCFpvoUhvw3LKH8PJz24O-oqF3DtQMi2hNhn9nPU/edit?tab=t.0

## example pids

dataset:
http://fox.cloud.dkrz.de:8008/api/handles/21.T14995/d7f1f9ad-189e-394b-9776-ed708d7fc9fe

file:
http://fox.cloud.dkrz.de:8008/api/handles/21.T14995/54b0261c-c7c4-3d60-839f-39acf20412ec

## esgf stac schema notes

### replica

GL: Regarding the replica: true/false field, I would suggest we align with the STAC specification by using the roles property of each asset to indicate whether it is a reference (master) or a replica. Specifically, we could adopt the following convention: roles: ["data", "reference"] for the canonical version of the dataset (as proposed in the JSON example) and roles: ["data", "replica"] for secondary copies. So the replica field will be dropped. Do we agree on this?

### size, checksum

GL: Yes, as the file:size will be handled by the file STAC extension, it’s better to have another field for the dataset size. I propose simply size as it cannot be confused with the assets sizes in the schema. Do we agree on this ?


### publication timestamp

SA can you confirm the publication timestamp is automatically set up and does not need to be validated by the STAC extension?


 "metadata": {
        "auth": {
            "auth_policy_id": null,
            "requester_data": {
                "client_id": "ec5f07c0-7ed8-4f2b-94f2-ddb6f8fc91a3",
                "iss": "https://auth.globus.org",
                "sub": "a511c7bc-d274-11e5-9aea-4bedf3cb22c7"
            }
        },
        "event_id": "932bda79fec7461abb82f17d7d951072",
        "publisher": {
            "package": "test_client",
            "version": "0.1.0"
        },
        "request_id": "25fce4cc536847cea76418b2fba5b8ed",
        "time": "2025-08-06T16:16:37.933751",
        "schema_version": "1.0.0"
    },

### tracking_id

part of file asset ...

"data0001": {
      "href": "https://dap.ceda.ac.uk/badc/cmip6/data/CMIP6/ScenarioMIP/THU/CIESM/ssp585/r1i1p1f1/Amon/rsus/gr/v20200806/rsus_Amon_CIESM_ssp585_r1i1p1f1_gr_402901-411412.nc",
      "type": "application/netcdf",
      "file:checksum": "90e402107a7f2588a85362b9beea2a12d4514d45",
      "tracking_id": "hdl:21.14100/7a8097a5-3ebb-4491-8640-01843dbdecd2",
      "roles": [
        "data"
      ]
    }





## check official PID

https://hdl.handle.net/21.T14995/3bc78243-6735-30fb-80c0-d6382f89a1b8?noredirect


## build PID

uuid.uuid3(uuid.NAMESPACE_URL, "CMIP6.C4MIP.NOAA-GFDL.GFDL-ESM4.1pctCO2-bgc.r1i1p1f1.Amon.ps.gr1.v20180701")

cmip7 prefix: hdl:21.14107/

test prefix: hdl:21.T14995/UUID

## patches

### retracted

"patch": {"operations": [{"op": "add", "path": "/properties/retracted", "value": true}]}

### add asset

"patch": {"operations": [{"op": "add", "path": "/assets/globus"

"patch": {"operations": [{"op": "add", "path": "/assets/data0000"

### bbox

"patch": {"operations": [{"path": "/bbox", "op": "add", "value": [-180.0, -45.0, 180, 90]}]}

### variable name

 "patch": {"operations": [{"path": "/properties/variable_long_name", "op": "add", "value": "Precipitation"}

## updates

### 2025-11-03

checksum have encoded checksum type:

https://github.com/multiformats/multihash

It needs to be separated from the checksum and stored as the checksum type.

### 2025-10-29
Moin Carsten, 

die wollen demnächst einen größeren Lasttest machen und haben mich gefragt, ob folgende Metadaten für uns ok sind:

```
$ cat CMIP6.AerChemMIP.BCC.BCC-ESM1.hist-piAer.r3i1p1f1.Amon.hus.gn.v20200430_eagle.alcf.anl.gov.json
{
    "type": "Feature",
    "stac_version": "1.1.0",
    "stac_extensions": [
        "https://esgf.github.io/stac-transaction-api/cmip6/v1.0.0/schema.json",
        "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        "https://stac-extensions.github.io/file/v2.1.0/schema.json"
    ],
    "id": "CMIP6.AerChemMIP.BCC.BCC-ESM1.hist-piAer.r3i1p1f1.Amon.hus.gn.v20200430",
    "geometry": {
        "type": "Polygon",
        "coordinates": [...]
    },
    "bbox": [...],
    "collection": "CMIP6",
    "links": [...],
    "properties": {
        "datetime": null,
        "start_datetime": "1850-01-16T12:00:00Z",
        "end_datetime": "2014-11-06T12:00:00Z",
        "size": 1232798856,
        "created": "2025-01-02T00:00:00Z",
        "updated": "2025-01-02T00:00:00Z",
        "access": [
            "HTTPServer",
            "Globus"
        ],
        "latest": true,
        "pid": "hdl:21.14100/e4d72478-e3f9-3f7e-9088-74b615d09a78",
        "project": "CMIP6",
        "retracted": false,
        "title": "CMIP6.AerChemMIP.BCC.BCC-ESM1.hist-piAer.r3i1p1f1.Amon.hus.gn",
        "version": 20200430,
        "cmip6:activity_id": "AerChemMIP",
        "cmip6:cf_standard_name": "specific_humidity",
        "cmip6:citation_url": "http://cera-www.dkrz.de/WDCC/meta/CMIP6/CMIP6.AerChemMIP.BCC.BCC-ESM1.hist-piAer.r3i1p1f1.Amon.hus.gn.v20200430.json",
        "cmip6:data_specs_version": "01.00.27",
        "cmip6:experiment_id": "hist-piAer",
        "cmip6:experiment_title": "historical forcing, but with pre-industrial aerosol emissions",
        "cmip6:frequency": "mon",
        "cmip6:further_info_url": "https://furtherinfo.es-doc.org/CMIP6.BCC.BCC-ESM1.hist-piAer.none.r3i1p1f1",
        "cmip6:grid": "T42",
        "cmip6:grid_label": "gn",
        "cmip6:institution_id": "BCC",
        "cmip6:member_id": "r3i1p1f1",
        "cmip6:mip_era": "CMIP6",
        "cmip6:nominal_resolution": "250 km",
        "cmip6:product": "model-output",
        "cmip6:realm": [
            "atmos"
        ],
        "cmip6:source_id": "BCC-ESM1",
        "cmip6:source_type": [
            "AER",
            "AOGCM",
            "CHEM"
        ],
        "cmip6:table_id": "Amon",
        "cmip6:variable": "hus",
        "cmip6:variable_long_name": "Specific Humidity",
        "cmip6:variable_units": "1",
        "cmip6:variant_label": "r3i1p1f1"
    },
    "assets": {
        "globus": {
            "href": "https://app.globus.org/file-manager?origin_id=8896f38e-68d1-4708-bce4-b1b3a3405809&origin_path=/css03_data/CMIP6/AerChemMIP/BCC/BCC-ESM1/hist-piAer/r3i1p1f1/Amon/hus/gn/v20200430/",
            "description": "Globus Web App Link",
            "type": "text/html",
            "roles": [
                "data"
            ],
            "alternate:name": "eagle.alcf.anl.gov"
        },
        "data0000": {
            "href": "https://g-52ba3.fd635.8443.data.globus.org/css03_data/CMIP6/AerChemMIP/BCC/BCC-ESM1/hist-piAer/r3i1p1f1/Amon/hus/gn/v20200430/hus_Amon_BCC-ESM1_hist-piAer_r3i1p1f1_gn_185001-201412.nc",
            "description": "HTTPServer Link",
            "type": "application/netcdf",
            "roles": [
                "data"
            ],
            "alternate:name": "eagle.alcf.anl.gov",
            "file:size": 1232798856,
            "file:checksum": "1220ebde086005717110daef5f5b7fc9084a2aa9a91428baaa72ad1e4e2337571762",
            "cmip6:tracking_id": "hdl:21.14100/8327d538-67a9-4460-99a5-fb22ebbc2148"
        }
    }
}
```

### 2025-10-02

Die STAC Kataloge und Suche für DC4 und ESGF West sind hier zu finden: https://data-challenge-04-discovery.api.stac.esgf-west.org/

Und die nächste DC wird ein Load Test mit > 100.000 items sein. Aber wenn ich es richtig gesehen habe keine neue versionen sondern nur neu-Publikationen und Updated.

### 2025-09-23

* update tests
* host = unknown: treat as error?
* check version lookup ... use mock test?
* retracted, replica ... use differnt names
* how to handle checksum types?
* how to use strict (validation) mode?
* when strict = raise error, when not strict = raise warning
* need a stac synced with kafka for testing
* need multiple datasetid versions in kafka stream
* work on recovery ... might also be used for "offline" testing ...
* need cli for evaluation of stats.db


### 2025-09-11

dataset:
* use retraced_on with timestamp instead of retraced 
    * only set when retracted

file: 
* add download_replica url

version lookup:
* Use cached ES for handle service.

github:
* move code to ESGF repo (piddiplatsch)