import ExtractTransformLoad from "./ExtractTransformLoad";

const rootZipFile = "files/input.zip";
const decompressedFiles = "files/decompressed";

const inputDir = decompressedFiles + "/input";
const transformedFiles = "files/transformed";

const ETL = new ExtractTransformLoad(
  rootZipFile,
  decompressedFiles,
  inputDir,
  transformedFiles
);

ETL.transformJSON();
