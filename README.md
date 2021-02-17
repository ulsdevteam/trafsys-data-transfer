# trafsys-data-transfer



## Usage

Install using `npm install`. You can specify the time period using the `--from` and `--to` options. If either of these are not specified, it will default to the previous day. These values are passed directly to the TrafSys API, so if they are formatted incorrectly, or if the From Date is after the To Date, the API call will fail with a 400 or 500 error.

`node script.js --from 2020-01-01 --to 2020-12-31`

## License

Copyright University of Pittsburgh.

Freely licensed for reuse under the MIT License.