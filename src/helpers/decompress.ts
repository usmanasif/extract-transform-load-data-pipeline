import decompress from "decompress";

export default async function decompressDir(
  inputFile: string,
  outputDir: string
) {
  try {
    await decompress(inputFile, outputDir);
  } catch (error) {
    console.error(error);
  }
}
