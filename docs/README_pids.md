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

### 2025-09-23

* update tests
* host = unknown: treat as error?
* check version lookup ... use mock test?
* retracted, replica ... use differnt names
* how to handle checksum types?
* need a stac synced with kafka for testing
* need multiple datasetid versions in kafka stream


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