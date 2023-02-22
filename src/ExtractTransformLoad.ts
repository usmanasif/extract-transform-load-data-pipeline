import { existsSync, mkdirSync } from "fs";
import {
  createReadStream,
  createWriteStream,
  WriteStream,
  readdirSync,
} from "fs";
import path from "path";
import { Transform } from "stream";
import { createGunzip } from "zlib";
import decompressDir from "./helpers/decompress";
import { IOriginalObject, ITransformedObject } from "./types";

class ExtractTransformLoad {
  private inputFilesPath: string;
  private outputFilesPath: string;
  private rootZipFilePath: string;
  private uncompressedFilePath: string;
  private counter = 0;
  private EIGHT_KB = 8192;

  constructor(
    rootFilePath: string,
    uncompressedFilePath: string,
    inPath: string,
    outPath: string
  ) {
    this.inputFilesPath = inPath;
    this.outputFilesPath = outPath;
    this.rootZipFilePath = rootFilePath;
    this.uncompressedFilePath = uncompressedFilePath;
  }

  async decompressRoot() {
    return await decompressDir(this.rootZipFilePath, this.uncompressedFilePath);
  }

  async transformJSON() {
    // Decompress the root zip file
    await this.decompressRoot();

    if (!existsSync(this.outputFilesPath)) {
      mkdirSync(this.outputFilesPath);
    }

    return new Promise((resolve, reject) => {
      try {
        // Get the list of gzipped json files in the input directory
        const files = readdirSync(this.inputFilesPath);

        let outputFile: string;
        let uncompressedWriteStream: WriteStream;
        let totalSize = 0;
        let chunkArray: Array<ITransformedObject> = [];

        const createNewFile = () => {
          totalSize = 0;
          outputFile = path.join(this.outputFilesPath, `${this.counter}.json`);
          uncompressedWriteStream = createWriteStream(outputFile);
          this.counter++;
        };

        // Iterate through each file in the input directory
        files.forEach((file, index) => {
          const inputFile = path.join(this.inputFilesPath, file);

          // Create a read stream to read the compressed file
          const compressedReadStream = createReadStream(inputFile);

          // Create a decompression stream and pipe the input and output streams through it
          const decompressionStream = createGunzip();

          const transformStream = this.transformStream();

          compressedReadStream
            .pipe(decompressionStream)
            .pipe(transformStream)
            .on("data", (chunk) => {
              const size = Buffer.byteLength(chunk);
              if (totalSize + size > this.EIGHT_KB) {
                createNewFile();
                uncompressedWriteStream.write(JSON.stringify(chunkArray));
                totalSize += size;
                chunkArray = [];
              } else {
                const parsedChunk = JSON.parse(chunk);
                chunkArray.push(...parsedChunk);
                totalSize += size;
              }
              if (files.length === index + 1) {
                createNewFile();
                uncompressedWriteStream.write(JSON.stringify(chunkArray));
                chunkArray = [];
              }
            })
            .on("end", () => {
              resolve(null);
              uncompressedWriteStream.end();
            });
        });
      } catch (error) {
        console.error(error);
        reject(error);
      }
    });
  }

  transformStream() {
    const etl = this;
    return new Transform({
      transform(chunk, encoding, callback) {
        const obj: IOriginalObject = JSON.parse(chunk);

        const newObj: Omit<ITransformedObject, "ec"> = {
          url_object: etl.parseUrl(obj.u),
          timestamp: obj.ts,
        };

        const transformed: Array<ITransformedObject> = obj?.e?.map((e) => {
          return {
            ...newObj,
            ec: e,
          };
        });

        const modifiedChunk = JSON.stringify(transformed);
        this.push(modifiedChunk);
        callback();
      },
    });
  }

  parseUrl(url: string) {
    const { host, pathname, searchParams, hash } = new URL(url);

    return {
      domain: host,
      path: pathname,
      query_object: searchParams ? Object.fromEntries(searchParams) : {},
      hash: hash,
    };
  }
}
export default ExtractTransformLoad;
