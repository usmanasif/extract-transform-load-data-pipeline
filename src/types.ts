export interface ITransformedObject {
  timestamp: number;
  url_object: IUrlObject;
  ec: Record<string, unknown>;
}
export interface IUrlObject {
  domain: string;
  path: string;
  query_object: Record<string, string>;
  hash: string;
}

export interface IOriginalObject {
  ts: number;
  u: string;
  e: Array<Record<string, unknown>>;
}
