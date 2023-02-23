import { createReadStream, existsSync, readdirSync, unlinkSync } from "fs";
import path from "path";
import { createGunzip } from "zlib";
import ExtractTransformLoad from "../ExtractTransformLoad";
import { ITransformedObject } from "../types";

describe("Extract Transform Load", () => {
  const rootFilePath = path.resolve(__dirname + "/testFiles/input.zip");
  const uncompressedFilePath = path.resolve(
    __dirname + "/testFiles/uncompressed"
  );
  const inputFilesPath = path.resolve(__dirname + "/testFiles/uncompressed/");
  const outputFilesPath = path.resolve(__dirname + "/testFiles/output");

  let etl: ExtractTransformLoad;

  beforeEach(() => {
    etl = new ExtractTransformLoad(
      rootFilePath,
      uncompressedFilePath,
      inputFilesPath,
      outputFilesPath
    );
  });

  const resetFiles = async () => {
    if (existsSync(outputFilesPath)) {
      const outputFiles = readdirSync(outputFilesPath);
      for (const file of outputFiles) {
        unlinkSync(outputFilesPath + "/" + file);
      }
    }
    if (existsSync(uncompressedFilePath)) {
      const uncompressedFiles = readdirSync(uncompressedFilePath);
      for (const file of uncompressedFiles) {
        unlinkSync(uncompressedFilePath + "/" + file);
      }
    }
  };

  const getOutputFilesData = () => {
    const outputFiles = readdirSync(outputFilesPath);

    return new Promise<Array<ITransformedObject>>((resolve, reject) => {
      try {
        const temp: Array<ITransformedObject> = [];
        outputFiles.forEach((file) => {
          const outputFile = path.join(outputFilesPath, file);
          const f = createReadStream(outputFile);
          f.on("data", (chunk: string) => {
            const data = JSON.parse(chunk);
            temp.push(...data);
          });

          f.on("end", () => {
            resolve(temp);
          });
        });
      } catch (error) {
        reject(error);
      }
    });
  };

  const getGzippiedJsonFilesData = async () => {
    return new Promise<Array<ITransformedObject>>((resolve, reject) => {
      try {
        const temp: Array<ITransformedObject> = [];
        const files = readdirSync(inputFilesPath);
        files.forEach((file) => {
          const inputFile = path.join(inputFilesPath, file);

          const compressedReadStream = createReadStream(inputFile);

          const decompressionStream = createGunzip();

          const transformStream = etl.transformStream();

          compressedReadStream
            .pipe(decompressionStream)
            .pipe(transformStream)
            .on("data", (chunk) => {
              temp.push(...JSON.parse(chunk));
            })
            .on("end", () => {
              resolve(temp);
            });
        });
      } catch (error) {
        console.error(error);
        reject(error);
      }
    });
  };

  test("Should Parse Url into structured object", () => {
    const url = "https://example.com/path?foo=bar#section";
    const expected = {
      domain: "example.com",
      path: "/path",
      query_object: { foo: "bar" },
      hash: "#section",
    };
    expect(etl.parseUrl(url)).toEqual(expected);
  });

  test("Should transform a single json object", (done) => {
    const input = {
      ts: 1612486301,
      u: "https://example.com/",
      e: [
        {
          n: "pageview",
          i: { i: "qwe" },
        },
        {
          ne: "pageview qwe",
          wi: { i: "io qwe" },
        },
      ],
    };
    const expected = [
      {
        url_object: {
          domain: "example.com",
          path: "/",
          query_object: {},
          hash: "",
        },
        timestamp: 1612486301,
        ec: {
          n: "pageview",
          i: { i: "qwe" },
        },
      },
      {
        url_object: {
          domain: "example.com",
          path: "/",
          query_object: {},
          hash: "",
        },
        timestamp: 1612486301,
        ec: {
          ne: "pageview qwe",
          wi: { i: "io qwe" },
        },
      },
    ];

    const transformStream = etl.transformStream();
    const outputChunks: Array<string> = [];

    transformStream.on("data", (chunk: Buffer) => {
      outputChunks.push(chunk.toString());
    });

    transformStream.on("end", () => {
      const output = JSON.parse(outputChunks.join(""));
      expect(output).toEqual(expected);
      done();
    });

    transformStream.write(JSON.stringify(input));
    transformStream.end();
  });

  test("Check each output file size to be less than 8KB", async () => {
    await resetFiles();
    await etl.transformJSON();

    const outputData: Array<ITransformedObject> = await getOutputFilesData();

    outputData.forEach((data) =>
      expect(Buffer.byteLength(JSON.stringify(data))).toBeLessThan(8192)
    );
  });

  test("Should transform multiple gzipped JSON files correctly", async () => {
    await resetFiles();
    await etl.transformJSON();
    const testData: Array<ITransformedObject> =
      await getGzippiedJsonFilesData();
    const outputData: Array<ITransformedObject> = await getOutputFilesData();

    expect(outputData.length).toEqual(testData.length);
  });

  test("Should test one of the sample object to be in the output", async () => {
    await resetFiles();
    await etl.transformJSON();

    const expectedObject = {
      url_object: {
        domain: "www.imaginary.net",
        path: "/gudec",
        query_object: {
          var8: ",|Djl",
          var4: "l",
          k0: "xP",
          var2: "15",
        },
        hash: "#bcdgupm/h",
      },
      timestamp: 1669976043721,
      ec: {
        et: "dl",
        n: "digitalData",
        u: {
          page_name: "product",
          product_id: "P1497",
          product_price: 53,
        },
      },
    };

    const outputData: Array<ITransformedObject> = await getOutputFilesData();
    expect(outputData).toContainEqual(expectedObject);
  });
});
