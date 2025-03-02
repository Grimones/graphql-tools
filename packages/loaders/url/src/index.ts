/* eslint-disable no-case-declarations */
/// <reference lib="dom" />
import { print, IntrospectionOptions, DocumentNode, Kind, parse, buildASTSchema, GraphQLError } from 'graphql';
import {
  AsyncExecutor,
  Executor,
  Subscriber,
  SyncExecutor,
  SchemaPointerSingle,
  Source,
  DocumentLoader,
  SingleFileOptions,
  observableToAsyncIterable,
  isAsyncIterable,
  ExecutionParams,
  mapAsyncIterator,
  withCancel,
} from '@graphql-tools/utils';
import { isWebUri } from 'valid-url';
import { fetch as crossFetch, Headers } from 'cross-fetch';
import { SubschemaConfig } from '@graphql-tools/delegate';
import { introspectSchema, wrapSchema } from '@graphql-tools/wrap';
import { ClientOptions, createClient } from 'graphql-ws';
import WebSocket from 'isomorphic-ws';
import syncFetch from 'sync-fetch';
import isPromise from 'is-promise';
import { extractFiles, isExtractableFile } from 'extract-files';
import FormData from 'form-data';
import '@ardatan/eventsource';
import { Subscription, SubscriptionOptions } from 'sse-z';
import { URL } from 'url';
import { ConnectionParamsOptions, SubscriptionClient as LegacySubscriptionClient } from 'subscriptions-transport-ws';
import AbortController from 'abort-controller';
import { meros } from 'meros';
import { merge, set } from 'lodash';

export type AsyncFetchFn = typeof import('cross-fetch').fetch;
export type SyncFetchFn = (input: RequestInfo, init?: RequestInit) => SyncResponse;
export type SyncResponse = Omit<Response, 'json' | 'text'> & {
  json: () => any;
  text: () => string;
};
export type FetchFn = AsyncFetchFn | SyncFetchFn;

type Headers =
  | Record<string, string>
  | Array<Record<string, string>>
  | ((executionParams: ExecutionParams) => Array<Record<string, string>> | Record<string, string>);

type BuildExecutorOptions<TFetchFn = FetchFn> = {
  pointer: string;
  fetch: TFetchFn;
  extraHeaders: Headers;
  defaultMethod: 'GET' | 'POST';
  useGETForQueries: boolean;
  multipart?: boolean;
};

export type AsyncImportFn<T = unknown> = (moduleName: string) => PromiseLike<T>;
export type SyncImportFn<T = unknown> = (moduleName: string) => T;

const asyncImport: AsyncImportFn = (moduleName: string) => import(moduleName);
const syncImport: SyncImportFn = (moduleName: string) => require(moduleName);

interface ExecutionResult<TData = { [key: string]: any }, TExtensions = { [key: string]: any }> {
  errors?: ReadonlyArray<GraphQLError>;
  data?: TData | null;
  extensions?: TExtensions;
}

interface ExecutionPatchResult<TData = { [key: string]: any }, TExtensions = { [key: string]: any }> {
  errors?: ReadonlyArray<GraphQLError>;
  data?: TData | null;
  path?: ReadonlyArray<string | number>;
  label?: string;
  hasNext: boolean;
  extensions?: TExtensions;
}

/**
 * Additional options for loading from a URL
 */
export interface LoadFromUrlOptions extends SingleFileOptions, Partial<IntrospectionOptions> {
  /**
   * Additional headers to include when querying the original schema
   */
  headers?: Headers;
  /**
   * A custom `fetch` implementation to use when querying the original schema.
   * Defaults to `cross-fetch`
   */
  customFetch?: FetchFn | string;
  /**
   * HTTP method to use when querying the original schema.
   */
  method?: 'GET' | 'POST';
  /**
   * Custom WebSocket implementation used by the loaded schema if subscriptions
   * are enabled
   */
  webSocketImpl?: typeof WebSocket | string;
  /**
   * Whether to use the GET HTTP method for queries when querying the original schema
   */
  useGETForQueries?: boolean;
  /**
   * Use multipart for POST requests
   */
  multipart?: boolean;
  /**
   * Use SSE for subscription instead of WebSocket
   */
  useSSEForSubscription?: boolean;
  /**
   * Use legacy web socket protocol `graphql-ws` instead of the more current standard `graphql-transport-ws`
   */
  useWebSocketLegacyProtocol?: boolean;
  /**
   * Additional options to pass to the constructor of the underlying EventSource instance.
   */
  eventSourceOptions?: SubscriptionOptions['eventSourceOptions'];
  /**
   * Handle URL as schema SDL
   */
  handleAsSDL?: boolean;
  /**
   * Subscriptions endpoint; defaults to the endpoint given as pointer
   */
  subscriptionsEndpoint?: string;
}

/**
 * This loader loads a schema from a URL. The loaded schema is a fully-executable,
 * remote schema since it's created using [@graphql-tools/wrap](/docs/remote-schemas).
 *
 * ```
 * const schema = await loadSchema('http://localhost:3000/graphql', {
 *   loaders: [
 *     new UrlLoader(),
 *   ]
 * });
 * ```
 */
export class UrlLoader implements DocumentLoader<LoadFromUrlOptions> {
  loaderId(): string {
    return 'url';
  }

  async canLoad(pointer: SchemaPointerSingle, options: LoadFromUrlOptions): Promise<boolean> {
    return this.canLoadSync(pointer, options);
  }

  canLoadSync(pointer: SchemaPointerSingle, _options: LoadFromUrlOptions): boolean {
    return !!isWebUri(pointer);
  }

  async createFormDataFromVariables<TVariables>({ query, variables }: { query: string; variables: TVariables }) {
    const vars = Object.assign({}, variables);
    const { clone, files } = extractFiles(
      vars,
      'variables',
      ((v: any) => isExtractableFile(v) || v?.promise || isAsyncIterable(v) || isPromise(v)) as any
    );
    const map = Array.from(files.values()).reduce((prev, curr, currIndex) => {
      prev[currIndex] = curr;
      return prev;
    }, {});
    const uploads: any = new Map(Array.from(files.keys()).map((u, i) => [i, u]));
    const form = new FormData();
    form.append(
      'operations',
      JSON.stringify({
        query,
        variables: clone,
      })
    );
    form.append('map', JSON.stringify(map));
    await Promise.all(
      Array.from(uploads.entries()).map(async ([i, u]) => {
        if (isPromise(u)) {
          u = await u;
        }
        if (u?.promise) {
          const upload = await u.promise;
          const stream = upload.createReadStream();
          form.append(i.toString(), stream, {
            filename: upload.filename,
            contentType: upload.mimetype,
          } as any);
        } else {
          form.append(
            i.toString(),
            u as any,
            {
              filename: 'name' in u ? u['name'] : i,
              contentType: u.type,
            } as any
          );
        }
      })
    );
    return form;
  }

  buildExecutor(options: BuildExecutorOptions<SyncFetchFn>): SyncExecutor;
  buildExecutor(options: BuildExecutorOptions<AsyncFetchFn>): AsyncExecutor;
  buildExecutor({
    pointer,
    fetch,
    extraHeaders,
    defaultMethod,
    useGETForQueries,
    multipart,
  }: BuildExecutorOptions): Executor {
    const HTTP_URL = switchProtocols(pointer, {
      wss: 'https',
      ws: 'http',
    });
    const executor = ({ document, variables, ...rest }: ExecutionParams) => {
      const controller = new AbortController();
      let method = defaultMethod;
      if (useGETForQueries) {
        method = 'GET';
        for (const definition of document.definitions) {
          if (definition.kind === Kind.OPERATION_DEFINITION) {
            if (definition.operation !== 'query') {
              method = defaultMethod;
            }
          }
        }
      }

      const headers = this.getHeadersFromOptions(extraHeaders, {
        document,
        variables,
        ...rest,
      });

      let fetchResult: SyncResponse | Promise<Response>;
      const query = print(document);
      switch (method) {
        case 'GET':
          const dummyHostname = 'https://dummyhostname.com';
          const validUrl = HTTP_URL.startsWith('http') ? HTTP_URL : `${dummyHostname}/${HTTP_URL}`;
          const urlObj = new URL(validUrl);
          urlObj.searchParams.set('query', query);
          if (variables && Object.keys(variables).length > 0) {
            urlObj.searchParams.set('variables', JSON.stringify(variables));
          }
          const finalUrl = urlObj.toString().replace(dummyHostname, '');
          fetchResult = fetch(finalUrl, {
            method: 'GET',
            credentials: 'include',
            headers: {
              accept: 'application/json',
              ...headers,
            },
          });
          break;
        case 'POST':
          if (multipart) {
            fetchResult = this.createFormDataFromVariables({ query, variables }).then(form =>
              (fetch as AsyncFetchFn)(HTTP_URL, {
                method: 'POST',
                credentials: 'include',
                body: form as any,
                headers: {
                  accept: 'application/json',
                  ...headers,
                },
                signal: controller.signal,
              })
            );
          } else {
            fetchResult = fetch(HTTP_URL, {
              method: 'POST',
              credentials: 'include',
              body: JSON.stringify({
                query,
                variables,
              }),
              headers: {
                accept: 'application/json, multipart/mixed',
                'content-type': 'application/json',
                ...headers,
              },
              signal: controller.signal,
            });
          }
          break;
      }
      if (isPromise(fetchResult)) {
        return fetchResult.then(async res => {
          const response: ExecutionResult = {};
          const maybeStream = await meros<ExecutionPatchResult>(res);
          if (isAsyncIterable(maybeStream)) {
            return withCancel(
              mapAsyncIterator(maybeStream, part => {
                if (part.json) {
                  const chunk = part.body;
                  if (chunk.path) {
                    if (chunk.data) {
                      const path: Array<string | number> = ['data'];
                      merge(response, set({}, path.concat(chunk.path), chunk.data));
                    }

                    if (chunk.errors) {
                      response.errors = (response.errors || []).concat(chunk.errors);
                    }
                  } else {
                    if (chunk.data) {
                      response.data = chunk.data;
                    }
                    if (chunk.errors) {
                      response.errors = chunk.errors;
                    }
                  }
                  return response;
                }
              }),
              () => controller.abort()
            );
          } else {
            return maybeStream.json();
          }
        });
      }
      return fetchResult.json();
    };

    return executor;
  }

  buildWSSubscriber(
    subscriptionsEndpoint: string,
    webSocketImpl: typeof WebSocket,
    connectionParams?: ClientOptions['connectionParams']
  ): Subscriber {
    const WS_URL = switchProtocols(subscriptionsEndpoint, {
      https: 'wss',
      http: 'ws',
    });
    const subscriptionClient = createClient({
      url: WS_URL,
      webSocketImpl,
      connectionParams,
    });
    return async ({ document, variables }: { document: DocumentNode; variables: any }) => {
      const query = print(document);
      return observableToAsyncIterable({
        subscribe: observer => {
          const unsubscribe = subscriptionClient.subscribe(
            {
              query,
              variables,
            },
            observer
          );
          return {
            unsubscribe,
          };
        },
      });
    };
  }

  buildWSLegacySubscriber(
    subscriptionsEndpoint: string,
    webSocketImpl: typeof WebSocket,
    connectionParams?: ConnectionParamsOptions
  ): Subscriber {
    const WS_URL = switchProtocols(subscriptionsEndpoint, {
      https: 'wss',
      http: 'ws',
    });
    const subscriptionClient = new LegacySubscriptionClient(
      WS_URL,
      {
        connectionParams,
      },
      webSocketImpl
    );

    return async <TReturn, TArgs>({ document, variables }: ExecutionParams<TArgs>) => {
      return observableToAsyncIterable(
        subscriptionClient.request({
          query: document,
          variables,
        })
      ) as AsyncIterator<ExecutionResult<TReturn>>;
    };
  }

  buildSSESubscriber(pointer: string, eventSourceOptions?: SubscriptionOptions['eventSourceOptions']): Subscriber {
    return async ({ document, variables }: { document: DocumentNode; variables: any }) => {
      const query = print(document);
      return observableToAsyncIterable({
        subscribe: observer => {
          const subscription = new Subscription({
            url: pointer,
            searchParams: {
              query,
              variables: JSON.stringify(variables),
            },
            eventSourceOptions: {
              // Ensure cookies are included with the request
              withCredentials: true,
              ...eventSourceOptions,
            },
            onNext: data => {
              const parsedData = JSON.parse(data);
              observer.next(parsedData);
            },
            onError: data => {
              observer.error(data);
            },
            onComplete: () => {
              observer.complete();
            },
          });
          return subscription;
        },
      });
    };
  }

  getFetch(
    customFetch: LoadFromUrlOptions['customFetch'],
    importFn: AsyncImportFn,
    async: true
  ): PromiseLike<AsyncFetchFn>;

  getFetch(customFetch: LoadFromUrlOptions['customFetch'], importFn: SyncImportFn, async: false): SyncFetchFn;

  getFetch(
    customFetch: LoadFromUrlOptions['customFetch'],
    importFn: SyncImportFn | AsyncImportFn,
    async: boolean
  ): SyncFetchFn | PromiseLike<AsyncFetchFn> {
    if (customFetch) {
      if (typeof customFetch === 'string') {
        const [moduleName, fetchFnName] = customFetch.split('#');
        const moduleResult = importFn(moduleName);
        if (isPromise(moduleResult)) {
          return moduleResult.then(module => (fetchFnName ? module[fetchFnName] : module));
        } else {
          return fetchFnName ? moduleResult[fetchFnName] : moduleResult;
        }
      } else {
        return customFetch as any;
      }
    }
    return async ? crossFetch : syncFetch;
  }

  private getHeadersFromOptions(customHeaders: Headers, executionParams: ExecutionParams): Record<string, string> {
    let headers = {};
    if (customHeaders) {
      if (typeof customHeaders === 'function') {
        customHeaders = customHeaders(executionParams);
      }
      if (Array.isArray(customHeaders)) {
        headers = customHeaders.reduce((prev: any, v: any) => ({ ...prev, ...v }), {});
      } else if (typeof customHeaders === 'object') {
        headers = customHeaders;
      }
    }
    return headers;
  }

  private getDefaultMethodFromOptions(method: LoadFromUrlOptions['method'], defaultMethod: 'GET' | 'POST') {
    if (method) {
      defaultMethod = method;
    }
    return defaultMethod;
  }

  getWebSocketImpl(options: LoadFromUrlOptions, importFn: AsyncImportFn): PromiseLike<typeof WebSocket>;

  getWebSocketImpl(options: LoadFromUrlOptions, importFn: SyncImportFn): typeof WebSocket;

  getWebSocketImpl(
    options: LoadFromUrlOptions,
    importFn: SyncImportFn | AsyncImportFn
  ): typeof WebSocket | PromiseLike<typeof WebSocket> {
    if (typeof options?.webSocketImpl === 'string') {
      const [moduleName, webSocketImplName] = options.webSocketImpl.split('#');
      const importedModule = importFn(moduleName);
      if (isPromise(importedModule)) {
        return importedModule.then(webSocketImplName ? importedModule[webSocketImplName] : importedModule);
      } else {
        return webSocketImplName ? importedModule[webSocketImplName] : importedModule;
      }
    } else {
      const websocketImpl = options.webSocketImpl || WebSocket;
      return websocketImpl;
    }
  }

  async getExecutorAndSubscriberAsync(
    pointer: SchemaPointerSingle,
    options: LoadFromUrlOptions = {}
  ): Promise<{ executor: AsyncExecutor; subscriber: Subscriber }> {
    const fetch = await this.getFetch(options.customFetch, asyncImport, true);
    const defaultMethod = this.getDefaultMethodFromOptions(options.method, 'POST');

    const executor = this.buildExecutor({
      pointer,
      fetch,
      extraHeaders: options.headers,
      defaultMethod,
      useGETForQueries: options.useGETForQueries,
      multipart: options.multipart,
    });

    let subscriber: Subscriber;

    const subscriptionsEndpoint = options.subscriptionsEndpoint || pointer;
    if (options.useSSEForSubscription) {
      subscriber = this.buildSSESubscriber(subscriptionsEndpoint, options.eventSourceOptions);
    } else {
      const webSocketImpl = await this.getWebSocketImpl(options, asyncImport);
      const connectionParams = () => ({ headers: this.getHeadersFromOptions(options.headers, {} as any) });
      if (options.useWebSocketLegacyProtocol) {
        subscriber = this.buildWSLegacySubscriber(subscriptionsEndpoint, webSocketImpl, connectionParams);
      } else {
        subscriber = this.buildWSSubscriber(subscriptionsEndpoint, webSocketImpl, connectionParams);
      }
    }

    return {
      executor,
      subscriber,
    };
  }

  getExecutorAndSubscriberSync(
    pointer: SchemaPointerSingle,
    options: LoadFromUrlOptions
  ): { executor: SyncExecutor; subscriber: Subscriber } {
    const fetch = this.getFetch(options?.customFetch, syncImport, false);
    const defaultMethod = this.getDefaultMethodFromOptions(options?.method, 'POST');

    const executor = this.buildExecutor({
      pointer,
      fetch,
      extraHeaders: options.headers,
      defaultMethod,
      useGETForQueries: options.useGETForQueries,
    });

    const subscriptionsEndpoint = options.subscriptionsEndpoint || pointer;
    let subscriber: Subscriber;
    if (options.useSSEForSubscription) {
      subscriber = this.buildSSESubscriber(subscriptionsEndpoint, options.eventSourceOptions);
    } else {
      const webSocketImpl = this.getWebSocketImpl(options, syncImport);
      const connectionParams = () => ({ headers: this.getHeadersFromOptions(options.headers, {} as any) });
      if (options.useWebSocketLegacyProtocol) {
        subscriber = this.buildWSLegacySubscriber(subscriptionsEndpoint, webSocketImpl, connectionParams);
      } else {
        subscriber = this.buildWSSubscriber(subscriptionsEndpoint, webSocketImpl, connectionParams);
      }
    }

    return {
      executor,
      subscriber,
    };
  }

  async getSubschemaConfigAsync(pointer: SchemaPointerSingle, options: LoadFromUrlOptions): Promise<SubschemaConfig> {
    const { executor, subscriber } = await this.getExecutorAndSubscriberAsync(pointer, options);
    return {
      schema: await introspectSchema(executor, undefined, options as IntrospectionOptions),
      executor,
      subscriber,
    };
  }

  getSubschemaConfigSync(pointer: SchemaPointerSingle, options: LoadFromUrlOptions): SubschemaConfig {
    const { executor, subscriber } = this.getExecutorAndSubscriberSync(pointer, options);
    return {
      schema: introspectSchema(executor, undefined, options as IntrospectionOptions),
      executor,
      subscriber,
    };
  }

  async handleSDLAsync(pointer: SchemaPointerSingle, options: LoadFromUrlOptions): Promise<Source> {
    const fetch = await this.getFetch(options?.customFetch, asyncImport, true);
    const headers = this.getHeadersFromOptions(options?.headers, {} as any);
    const defaultMethod = this.getDefaultMethodFromOptions(options?.method, 'GET');
    const response = await fetch(pointer, {
      method: defaultMethod,
      headers,
    });
    const schemaString = await response.text();
    const document = parse(schemaString, options);
    const schema = buildASTSchema(document, options);
    return {
      location: pointer,
      rawSDL: schemaString,
      document,
      schema,
    };
  }

  handleSDLSync(pointer: SchemaPointerSingle, options: LoadFromUrlOptions): Source {
    const fetch = this.getFetch(options?.customFetch, syncImport, false);
    const headers = this.getHeadersFromOptions(options?.headers, {} as any);
    const defaultMethod = this.getDefaultMethodFromOptions(options?.method, 'GET');
    const response = fetch(pointer, {
      method: defaultMethod,
      headers,
    });
    const schemaString = response.text();
    const document = parse(schemaString, options);
    const schema = buildASTSchema(document, options);
    return {
      location: pointer,
      rawSDL: schemaString,
      document,
      schema,
    };
  }

  async load(pointer: SchemaPointerSingle, options: LoadFromUrlOptions): Promise<Source> {
    if (options?.handleAsSDL || pointer.endsWith('.graphql')) {
      return this.handleSDLAsync(pointer, options);
    }

    const subschemaConfig = await this.getSubschemaConfigAsync(pointer, options);

    const remoteExecutableSchema = wrapSchema(subschemaConfig);

    return {
      location: pointer,
      schema: remoteExecutableSchema,
    };
  }

  loadSync(pointer: SchemaPointerSingle, options: LoadFromUrlOptions): Source {
    if (options?.handleAsSDL || pointer.endsWith('.graphql')) {
      return this.handleSDLSync(pointer, options);
    }

    const subschemaConfig = this.getSubschemaConfigSync(pointer, options);

    const remoteExecutableSchema = wrapSchema(subschemaConfig);

    return {
      location: pointer,
      schema: remoteExecutableSchema,
    };
  }
}

function switchProtocols(pointer: string, protocolMap: Record<string, string>): string {
  const protocols: [string, string][] = Object.keys(protocolMap).map(source => [source, protocolMap[source]]);
  return protocols.reduce(
    (prev, [source, target]) => prev.replace(`${source}://`, `${target}://`).replace(`${source}:\\`, `${target}:\\`),
    pointer
  );
}
