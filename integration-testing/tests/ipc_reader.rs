use arrow::ipc::reader::{FileReader, StreamReader};
use arrow::util::test_util::arrow_test_data;
use arrow_integration_testing::read_gzip_json;
use std::fs::File;

#[test]
fn read_generated_files_014() {
    let testdata = arrow_test_data();
    let version = "0.14.1";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec![
        "generated_interval",
        "generated_datetime",
        "generated_dictionary",
        "generated_map",
        "generated_nested",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_primitive",
        "generated_decimal",
    ];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
            testdata, version, path
        ))
        .unwrap();

        let mut reader = FileReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    });
}

#[test]
#[should_panic(expected = "Big Endian is not supported for Decimal!")]
fn read_decimal_be_file_should_panic() {
    let testdata = arrow_test_data();
    let file = File::open(format!(
        "{}/arrow-ipc-stream/integration/1.0.0-bigendian/generated_decimal.arrow_file",
        testdata
    ))
    .unwrap();
    FileReader::try_new(file, None).unwrap();
}

#[test]
#[should_panic(
    expected = "Last offset 687865856 of Utf8 is larger than values length 41"
)]
fn read_dictionary_be_not_implemented() {
    // The offsets are not translated for big-endian files
    // https://github.com/apache/arrow-rs/issues/859
    let testdata = arrow_test_data();
    let file = File::open(format!(
        "{}/arrow-ipc-stream/integration/1.0.0-bigendian/generated_dictionary.arrow_file",
        testdata
    ))
    .unwrap();
    FileReader::try_new(file, None).unwrap();
}

#[test]
fn read_generated_be_files_should_work() {
    // complementary to the previous test
    let testdata = arrow_test_data();
    let paths = vec![
        "generated_interval",
        "generated_datetime",
        "generated_map",
        "generated_nested",
        "generated_null_trivial",
        "generated_null",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_primitive",
    ];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/1.0.0-bigendian/{}.arrow_file",
            testdata, path
        ))
        .unwrap();

        FileReader::try_new(file, None).unwrap();
    });
}

#[test]
fn projection_should_work() {
    // complementary to the previous test
    let testdata = arrow_test_data();
    let paths = vec![
        "generated_interval",
        "generated_datetime",
        "generated_map",
        "generated_nested",
        "generated_null_trivial",
        "generated_null",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_primitive",
    ];
    paths.iter().for_each(|path| {
        // We must use littleendian files here.
        // The offsets are not translated for big-endian files
        // https://github.com/apache/arrow-rs/issues/859
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/1.0.0-littleendian/{}.arrow_file",
            testdata, path
        ))
        .unwrap();

        let reader = FileReader::try_new(file, Some(vec![0])).unwrap();
        let datatype_0 = reader.schema().fields()[0].data_type().clone();
        reader.for_each(|batch| {
            let batch = batch.unwrap();
            assert_eq!(batch.columns().len(), 1);
            assert_eq!(datatype_0, batch.schema().fields()[0].data_type().clone());
        });
    });
}

#[test]
fn read_generated_streams_014() {
    let testdata = arrow_test_data();
    let version = "0.14.1";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec![
        "generated_interval",
        "generated_datetime",
        "generated_dictionary",
        "generated_map",
        "generated_nested",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_primitive",
        "generated_decimal",
    ];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.stream",
            testdata, version, path
        ))
        .unwrap();

        let mut reader = StreamReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
        // the next batch must be empty
        assert!(reader.next().is_none());
        // the stream must indicate that it's finished
        assert!(reader.is_finished());
    });
}

#[test]
fn read_generated_files_100() {
    let testdata = arrow_test_data();
    let version = "1.0.0-littleendian";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec![
        "generated_interval",
        "generated_datetime",
        "generated_dictionary",
        "generated_map",
        // "generated_map_non_canonical",
        "generated_nested",
        "generated_null_trivial",
        "generated_null",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_primitive",
    ];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
            testdata, version, path
        ))
        .unwrap();

        let mut reader = FileReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    });
}

#[test]
fn read_generated_streams_100() {
    let testdata = arrow_test_data();
    let version = "1.0.0-littleendian";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec![
        "generated_interval",
        "generated_datetime",
        "generated_dictionary",
        "generated_map",
        // "generated_map_non_canonical",
        "generated_nested",
        "generated_null_trivial",
        "generated_null",
        "generated_primitive_no_batches",
        "generated_primitive_zerolength",
        "generated_primitive",
    ];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.stream",
            testdata, version, path
        ))
        .unwrap();

        let mut reader = StreamReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
        // the next batch must be empty
        assert!(reader.next().is_none());
        // the stream must indicate that it's finished
        assert!(reader.is_finished());
    });
}

#[test]
fn read_generated_streams_200() {
    let testdata = arrow_test_data();
    let version = "2.0.0-compression";

    // the test is repetitive, thus we can read all supported files at once
    let paths = vec!["generated_lz4", "generated_zstd"];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.stream",
            testdata, version, path
        ))
        .unwrap();

        let mut reader = StreamReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
        // the next batch must be empty
        assert!(reader.next().is_none());
        // the stream must indicate that it's finished
        assert!(reader.is_finished());
    });
}

#[test]
fn read_generated_files_200() {
    let testdata = arrow_test_data();
    let version = "2.0.0-compression";
    // the test is repetitive, thus we can read all supported files at once
    let paths = vec!["generated_lz4", "generated_zstd"];
    paths.iter().for_each(|path| {
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
            testdata, version, path
        ))
        .unwrap();

        let mut reader = FileReader::try_new(file, None).unwrap();

        // read expected JSON output
        let arrow_json = read_gzip_json(version, path);
        assert!(arrow_json.equals_reader(&mut reader).unwrap());
    });
}
