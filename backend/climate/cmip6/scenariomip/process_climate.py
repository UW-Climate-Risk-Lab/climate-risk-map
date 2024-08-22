import xarray as xr
import boto3
import s3fs


def main(s3_uris: str, file_format: str, crs: str) -> xr.Dataset:

    data = []
    for uri in s3_uris:
        fs = s3fs.S3FileSystem()
        with fs.open(uri) as file:
            ds = xr.open_dataset(filename_or_obj=file, engine=file_format)
            data.append(ds)
            ds = xr.merge(data)
    ds


if __name__ == "__main__":
    main()
