# Data pipiline project

## Description

- This project performs Extract transform load on a set of files
- It loads a root zip file containg multiple gzipped json files
- Decompress each file and transform it into the required structure on the fly
- Write the tranformed data into files on disk such that a file does not exceed 8KB in size.
- Each transformed file can contain multiple json objects.
- Transforms them into more readable structure

## Project Startup

- Run `npm install` in the root directory of the project
- Run `npm start` or `npm start:dev` to watch for file changes

## Dependencies

- Node >= v18.13.0
- Typescript
- Jest
