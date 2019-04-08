// --------------------------------------------------------------------------------------
// Utilities for working with network, downloading resources with specified headers etc.
// --------------------------------------------------------------------------------------

namespace FSharp.Data

open System
open System.Globalization
open System.IO
open System.Net
open System.Text
open System.Text.RegularExpressions
open System.Threading
open System.Reflection
open System.Runtime.CompilerServices
open System.Runtime.InteropServices

/// The method to use in an HTTP request
module HttpMethod =

    // RFC 2626 specifies 8 methods

    /// Request information about the communication options available on the request/response chain identified by the URI
    let Options = "OPTIONS"
    /// Retrieve whatever information (in the form of an entity) is identified by the URI
    let Get = "GET"
    /// Identical to GET except that the server MUST NOT return a message-body in the response
    let Head = "HEAD"
    /// Requests that the server accepts the entity enclosed in the request as a
    /// new subordinate of the resource identified by the Request-URI in the Request-Line
    let Post = "POST"
    /// Requests that the enclosed entity be stored under the supplied Request-URI
    let Put = "PUT"
    /// Requests that the origin server deletes the resource identified by the Request-URI
    let Delete = "DELETE"
    /// Used to invoke a remote, application-layer loop- back of the request message
    let Trace = "TRACE"
    /// Reserved for use with a proxy that can dynamically switch to being a tunnel
    let Connect = "CONNECT"

    // RFC 4918 (WebDAV) adds 7 methods

    /// Retrieves properties defined on the resource identified by the request URI
    let PropFind = "PROPFIND"
    /// Processes instructions specified in the request body to set and/or remove properties defined on the resource identified by the request URI
    let PropPatch = "PROPPATCH"
    /// Creates a new collection resource at the location specified by the Request URI
    let MkCol = "MKCOL"
    /// Creates a duplicate of the source resource, identified by the Request-URI, in the destination resource, identified by the URI in the Destination header
    let Copy = "COPY"
    /// Logical equivalent of a copy, followed by consistency maintenance processing, followed by a delete of the source where all three actions are performed atomically
    let Move = "MOVE"
    /// Used to take out a lock of any access type on the resource identified by the request URI.
    let Lock = "LOCK"
    /// Removes the lock identified by the lock token from the request URI, and all other resources included in the lock
    let Unlock = "UNLOCK"

    // RFC 5789 adds one more

    /// Requests that the origin server applies partial modifications contained in the entity enclosed in the request to the resource identified by the request URI
    let Patch = "PATCH"

/// Headers that can be sent in an HTTP request
module HttpRequestHeaders =
    /// Content-Types that are acceptable for the response
    let Accept (contentType:string) = "Accept", contentType
    /// Character sets that are acceptable
    let AcceptCharset (characterSets:string) = "Accept-Charset", characterSets
    /// Acceptable version in time
    let AcceptDatetime (dateTime:DateTime) = "Accept-Datetime", dateTime.ToString("R", CultureInfo.InvariantCulture)
    /// List of acceptable encodings. See HTTP compression.
    let AcceptEncoding (encoding:string) = "Accept-Encoding", encoding
    /// List of acceptable human languages for response
    let AcceptLanguage (language:string) = "Accept-Language", language
    /// The Allow header, which specifies the set of HTTP methods supported.
    let Allow (methods:string) = "Allow", methods
    /// Authentication credentials for HTTP authentication
    let Authorization (credentials:string) = "Authorization", credentials
    /// Authentication header using Basic Auth encoding
    let BasicAuth (username:string) (password:string) =
        let base64Encode (s:string) =
            let bytes = Encoding.UTF8.GetBytes(s)
            Convert.ToBase64String(bytes)
        sprintf "%s:%s" username password |> base64Encode |> sprintf "Basic %s" |>  Authorization
    /// Used to specify directives that MUST be obeyed by all caching mechanisms along the request/response chain
    let CacheControl (control:string) = "Cache-Control", control
    /// What type of connection the user-agent would prefer
    let Connection (connection:string) = "Connection", connection
    /// Describes the placement of the content. Valid dispositions are: inline, attachment, form-data
    let ContentDisposition (placement: string, name: string option, fileName: string option) =
        let namePart = match name with Some n -> sprintf "; name=\"%s\"" n | None -> ""
        let fileNamePart = match fileName with Some n -> sprintf "; filename=\"%s\"" n | None -> ""
        "Content-Disposition", sprintf "%s%s%s" placement namePart fileNamePart
    /// The type of encoding used on the data
    let ContentEncoding (encoding:string) = "Content-Encoding", encoding
    /// The language the content is in
    let ContentLanguage (language:string) = "Content-Language", language
    /// An alternate location for the returned data
    let ContentLocation (location:string) = "Content-Location", location
    /// A Base64-encoded binary MD5 sum of the content of the request body
    let ContentMD5 (md5sum:string) = "Content-MD5", md5sum
    /// Where in a full body message this partial message belongs
    let ContentRange (range:string) = "Content-Range", range
    /// The MIME type of the body of the request (used with POST and PUT requests)
    let ContentType (contentType:string) = "Content-Type", contentType
    /// The MIME type of the body of the request (used with POST and PUT requests) with an explicit encoding
    let ContentTypeWithEncoding (contentType, charset:Encoding) = "Content-Type", sprintf "%s; charset=%s" contentType (charset.WebName)
    /// The date and time that the message was sent
    let Date (date:DateTime) = "Date", date.ToString("R", CultureInfo.InvariantCulture)
    /// Indicates that particular server behaviors are required by the client
    let Expect (behaviors:string) = "Expect", behaviors
    /// Gives the date/time after which the response is considered stale
    let Expires (dateTime:DateTime) = "Expires", dateTime.ToString("R", CultureInfo.InvariantCulture)
    /// The email address of the user making the request
    let From (email:string) = "From", email
    /// The domain name of the server (for virtual hosting), and the TCP port number on which the server is listening.
    /// The port number may be omitted if the port is the standard port for the service requested.
    let Host (host:string) = "Host", host
    /// Only perform the action if the client supplied entity matches the same entity on the server.
    /// This is mainly for methods like PUT to only update a resource if it has not been modified since the user last updated it. If-Match: "737060cd8c284d8af7ad3082f209582d" Permanent
    let IfMatch (entity:string) = "If-Match", entity
    /// Allows a 304 Not Modified to be returned if content is unchanged
    let IfModifiedSince (dateTime:DateTime) = "If-Modified-Since", dateTime.ToString("R", CultureInfo.InvariantCulture)
    /// Allows a 304 Not Modified to be returned if content is unchanged
    let IfNoneMatch (etag:string) = "If-None-Match", etag
    /// If the entity is unchanged, send me the part(s) that I am missing; otherwise, send me the entire new entity
    let IfRange (range:string) = "If-Range", range
    /// Only send the response if the entity has not been modified since a specific time
    let IfUnmodifiedSince (dateTime:DateTime) = "If-Unmodified-Since", dateTime.ToString("R", CultureInfo.InvariantCulture)
    /// Specifies a parameter used into order to maintain a persistent connection
    let KeepAlive (keepAlive:string) = "Keep-Alive", keepAlive
    /// Specifies the date and time at which the accompanying body data was last modified
    let LastModified (dateTime:DateTime) = "Last-Modified", dateTime.ToString("R", CultureInfo.InvariantCulture)
    /// Limit the number of times the message can be forwarded through proxies or gateways
    let MaxForwards (count:int) = "Max-Forwards", count.ToString()
    /// Initiates a request for cross-origin resource sharing (asks server for an 'Access-Control-Allow-Origin' response header)
    let Origin (origin:string) = "Origin", origin
    /// Implementation-specific headers that may have various effects anywhere along the request-response chain.
    let Pragma (pragma:string) = "Pragma", pragma
    /// Optional instructions to the server to control request processing. See RFC https://tools.ietf.org/html/rfc7240 for more details
    let Prefer (prefer:string) = "Prefer", prefer
    /// Authorization credentials for connecting to a proxy.
    let ProxyAuthorization (credentials:string) = "Proxy-Authorization", credentials
    /// Request only part of an entity. Bytes are numbered from 0
    let Range (start:int64, finish:int64) = "Range", sprintf "bytes=%d-%d" start finish
    /// This is the address of the previous web page from which a link to the currently requested page was followed. (The word "referrer" is misspelled in the RFC as well as in most implementations.)
    let Referer (referer:string) = "Referer", referer
    /// The transfer encodings the user agent is willing to accept: the same values as for the response header
    /// Transfer-Encoding can be used, plus the "trailers" value (related to the "chunked" transfer method) to
    /// notify the server it expects to receive additional headers (the trailers) after the last, zero-sized, chunk.
    let TE (te:string) = "TE", te
    /// The Trailer general field value indicates that the given set of header fields is present in the trailer of a message encoded with chunked transfer-coding
    let Trailer (trailer:string) = "Trailer", trailer
    /// The TransferEncoding header indicates the form of encoding used to safely transfer the entity to the user.  The valid directives are one of: chunked, compress, deflate, gzip, or identity.
    let TransferEncoding (directive: string) = "Transfer-Encoding", directive
    /// Microsoft extension to the HTTP specification used in conjunction with WebDAV functionality.
    let Translate (translate:string) = "Translate", translate
    /// Specifies additional communications protocols that the client supports.
    let Upgrade (upgrade:string) = "Upgrade", upgrade
    /// The user agent string of the user agent
    let UserAgent (userAgent:string) = "User-Agent", userAgent
    /// Informs the server of proxies through which the request was sent
    let Via (server:string) = "Via", server
    /// A general warning about possible problems with the entity body
    let Warning (message:string) = "Warning", message
    /// Override HTTP method.
    let XHTTPMethodOverride (httpMethod:string) = "X-HTTP-Method-Override", httpMethod

/// Headers that can be received in an HTTP response
module HttpResponseHeaders =
    /// Specifying which web sites can participate in cross-origin resource sharing
    let [<Literal>] AccessControlAllowOrigin = "Access-Control-Allow-Origin"
    /// What partial content range types this server supports
    let [<Literal>] AcceptRanges = "Accept-Ranges"
    /// The age the object has been in a proxy cache in seconds
    let [<Literal>] Age = "Age"
    /// Valid actions for a specified resource. To be used for a 405 Method not allowed
    let [<Literal>] Allow = "Allow"
    /// Tells all caching mechanisms from server to client whether they may cache this object. It is measured in seconds
    let [<Literal>] CacheControl = "Cache-Control"
    /// Options that are desired for the connection
    let [<Literal>] Connection = "Connection"
    /// The type of encoding used on the data. See HTTP compression.
    let [<Literal>] ContentEncoding = "Content-Encoding"
    /// The language the content is in
    let [<Literal>] ContentLanguage = "Content-Language"
    /// The length of the response body in octets (8-bit bytes)
    let [<Literal>] ContentLength = "Content-Length"
    /// An alternate location for the returned data
    let [<Literal>] ContentLocation = "Content-Location"
    /// A Base64-encoded binary MD5 sum of the content of the response
    let [<Literal>] ContentMD5 = "Content-MD5"
    /// An opportunity to raise a "File Download" dialogue box for a known MIME type with binary format or suggest a filename for dynamic content. Quotes are necessary with special characters.
    let [<Literal>] ContentDisposition = "Content-Disposition"
    /// Where in a full body message this partial message belongs
    let [<Literal>] ContentRange = "Content-Range"
    /// The MIME type of this content
    let [<Literal>] ContentType = "Content-Type"
    /// The date and time that the message was sent (in "HTTP-date" format as defined by RFC 2616)
    let [<Literal>] Date = "Date"
    /// An identifier for a specific version of a resource, often a message digest
    let [<Literal>] ETag = "ETag"
    /// Gives the date/time after which the response is considered stale
    let [<Literal>] Expires = "Expires"
    /// The last modified date for the requested object
    let [<Literal>] LastModified = "Last-Modified"
    /// Used to express a typed relationship with another resource, where the relation type is defined by RFC 5988
    let [<Literal>] Link = "Link"
    /// Used in redirection, or when a new resource has been created.
    let [<Literal>] Location = "Location"
    /// This header is supposed to set P3P policy
    let [<Literal>] P3P = "P3P"
    /// Implementation-specific headers that may have various effects anywhere along the request-response chain.
    let [<Literal>] Pragma = "Pragma"
    /// Request authentication to access the proxy.
    let [<Literal>] ProxyAuthenticate = "Proxy-Authenticate"
    /// Used in redirection, or when a new resource has been created. This refresh redirects after 5 seconds.
    let [<Literal>] Refresh = "Refresh"
    /// If an entity is temporarily unavailable, this instructs the client to try again later. Value could be a specified period of time (in seconds) or a HTTP-date.[28]
    let [<Literal>] RetryAfter = "Retry-After"
    /// A name for the server
    let [<Literal>] Server = "Server"
    /// An HTTP cookie
    let [<Literal>] SetCookie = "Set-Cookie"
    /// The HTTP status of the response
    let [<Literal>] Status = "Status"
    /// A HSTS Policy informing the HTTP client how long to cache the HTTPS only policy and whether this applies to subdomains.
    let [<Literal>] StrictTransportSecurity = "Strict-Transport-Security"
    /// The Trailer general field value indicates that the given set of header fields is present in the trailer of a message encoded with chunked transfer-coding.
    let [<Literal>] Trailer = "Trailer"
    /// The form of encoding used to safely transfer the entity to the user. Currently defined methods are: chunked, compress, deflate, gzip, identity.
    let [<Literal>] TransferEncoding = "Transfer-Encoding"
    /// Tells downstream proxies how to match future request headers to decide whether the cached response can be used rather than requesting a fresh one from the origin server.
    let [<Literal>] Vary = "Vary"
    /// Informs the client of proxies through which the response was sent.
    let [<Literal>] Via = "Via"
    /// A general warning about possible problems with the entity body.
    let [<Literal>] Warning = "Warning"
    /// Indicates the authentication scheme that should be used to access the requested entity.
    let [<Literal>] WWWAuthenticate = "WWW-Authenticate"

/// Status codes that can be received in an HTTP response
module HttpStatusCodes = 
    /// The server has received the request headers and the client should proceed to send the request body.
    let [<Literal>] Continue = 100
    /// The requester has asked the server to switch protocols and the server has agreed to do so.
    let [<Literal>] SwitchingProtocols = 101
    /// This code indicates that the server has received and is processing the request, but no response is available yet.
    let [<Literal>] Processing = 102
    /// Used to return some response headers before final HTTP message.
    let [<Literal>] EarlyHints = 103

    /// Standard response for successful HTTP requests.
    let [<Literal>] OK = 200
    /// The request has been fulfilled, resulting in the creation of a new resource.
    let [<Literal>] Created = 201
    /// The request has been accepted for processing, but the processing has not been completed.
    let [<Literal>] Accepted = 202
    /// The server is a transforming proxy (e.g. a Web accelerator) that received a 200 OK from its origin, but is returning a modified version of the origin's response.
    let [<Literal>] NonAuthoritativeInformation = 203
    /// The server successfully processed the request and is not returning any content.
    let [<Literal>] NoContent = 204
    /// The server successfully processed the request, but is not returning any content.
    let [<Literal>] ResetContent = 205
    /// The server is delivering only part of the resource (byte serving) due to a range header sent by the client.
    let [<Literal>] PartialContent = 206
    /// The message body that follows is by default an XML message and can contain a number of separate response codes, depending on how many sub-requests were made.
    let [<Literal>] MultiStatus = 207
    /// The members of a DAV binding have already been enumerated in a preceding part of the (multistatus) response, and are not being included again.
    let [<Literal>] AlreadyReported = 208
    /// The server has fulfilled a request for the resource, and the response is a representation of the result of one or more instance-manipulations applied to the current instance.
    let [<Literal>] IMUsed = 226

    /// Indicates multiple options for the resource from which the client may choose (via agent-driven content negotiation).
    let [<Literal>] MultipleChoices = 300
    /// This and all future requests should be directed to the given URI.
    let [<Literal>] MovedPermanently = 301
    /// Tells the client to look at (browse to) another url. 302 has been superseded by 303 and 307. 
    let [<Literal>] Found = 302
    /// The response to the request can be found under another URI using the GET method.
    let [<Literal>] SeeOther = 303
    /// Indicates that the resource has not been modified since the version specified by the request headers If-Modified-Since or If-None-Match.
    let [<Literal>] NotModified = 304
    /// The requested resource is available only through a proxy, the address for which is provided in the response. 
    let [<Literal>] UseProxy = 305
    /// No longer used. Originally meant "Subsequent requests should use the specified proxy."
    let [<Literal>] SwitchProxy = 306
    /// In this case, the request should be repeated with another URI; however, future requests should still use the original URI.
    let [<Literal>] TemporaryRedirect = 307
    /// The request and all future requests should be repeated using another URI. 
    let [<Literal>] PermanentRedirect = 308

    /// The server cannot or will not process the request due to an apparent client error.
    let [<Literal>] BadRequest = 400
    /// Similar to 403 Forbidden, but specifically for use when authentication is required and has failed or has not yet been provided.
    let [<Literal>] Unauthorized = 401
    /// Reserved for future use. 
    let [<Literal>] PaymentRequired = 402
    /// The request was valid, but the server is refusing action. The user might not have the necessary permissions for a resource, or may need an account of some sort.
    let [<Literal>] Forbidden = 403
    /// The requested resource could not be found but may be available in the future. Subsequent requests by the client are permissible.
    let [<Literal>] NotFound = 404
    /// A request method is not supported for the requested resource.
    let [<Literal>] MethodNotAllowed = 405
    /// The requested resource is capable of generating only content not acceptable according to the Accept headers sent in the request.
    let [<Literal>] NotAcceptable = 406
    /// The client must first authenticate itself with the proxy.
    let [<Literal>] ProxyAuthenticationRequired = 407
    /// The server timed out waiting for the request.
    let [<Literal>] RequestTimeout = 408
    /// Indicates that the request could not be processed because of conflict in the request, such as an edit conflict between multiple simultaneous updates.
    let [<Literal>] Conflict = 409
    /// Indicates that the resource requested is no longer available and will not be available again.
    let [<Literal>] Gone = 410
    /// The request did not specify the length of its content, which is required by the requested resource.
    let [<Literal>] LengthRequired = 411
    /// The server does not meet one of the preconditions that the requester put on the request.
    let [<Literal>] PreconditionFailed = 412
    /// The request is larger than the server is willing or able to process.
    let [<Literal>] PayloadTooLarge = 413
    /// The URI provided was too long for the server to process.
    let [<Literal>] URITooLong = 414
    /// The request entity has a media type which the server or resource does not support.
    let [<Literal>] UnsupportedMediaType = 415
    /// The client has asked for a portion of the file (byte serving), but the server cannot supply that portion.
    let [<Literal>] RangeNotSatisfiable = 416
    /// The server cannot meet the requirements of the Expect request-header field.
    let [<Literal>] ExpectationFailed = 417
    /// The request was directed at a server that is not able to produce a response.
    let [<Literal>] MisdirectedRequest = 421
    /// The request was well-formed but was unable to be followed due to semantic errors.
    let [<Literal>] UnprocessableEntity = 422
    /// The resource that is being accessed is locked.
    let [<Literal>] Locked = 423
    /// The request failed because it depended on another request and that request failed (e.g., a PROPPATCH).
    let [<Literal>] FailedDependency = 424
    /// The client should switch to a different protocol such as TLS/1.0, given in the Upgrade header field.
    let [<Literal>] UpgradeRequired = 426
    /// The origin server requires the request to be conditional.
    let [<Literal>] PreconditionRequired = 428
    /// The user has sent too many requests in a given amount of time.
    let [<Literal>] TooManyRequests = 429
    /// The server is unwilling to process the request because either an individual header field, or all the header fields collectively, are too large.
    let [<Literal>] RequestHeaderFieldsTooLarge = 431
    /// A server operator has received a legal demand to deny access to a resource or to a set of resources that includes the requested resource.
    let [<Literal>] UnavailableForLegalReasons = 451

    /// A generic error message, given when an unexpected condition was encountered and no more specific message is suitable.
    let [<Literal>] InternalServerError = 500 
    /// The server either does not recognize the request method, or it lacks the ability to fulfil the request. 
    let [<Literal>] NotImplemented = 501
    /// The server was acting as a gateway or proxy and received an invalid response from the upstream server.
    let [<Literal>] BadGateway = 502
    /// The server is currently unavailable (because it is overloaded or down for maintenance).
    let [<Literal>] ServiceUnavailable = 503
    /// The server was acting as a gateway or proxy and did not receive a timely response from the upstream server.
    let [<Literal>] GatewayTimeout = 504
    /// The server does not support the HTTP protocol version used in the request.
    let [<Literal>] HTTPVersionNotSupported = 505 
    /// Transparent content negotiation for the request results in a circular reference.
    let [<Literal>] VariantAlsoNegotiates = 506
    /// The server is unable to store the representation needed to complete the request.
    let [<Literal>] InsufficientStorage = 507
    /// The server detected an infinite loop while processing the request.
    let [<Literal>] LoopDetected = 508
    /// Further extensions to the request are required for the server to fulfil it.
    let [<Literal>] NotExtended = 510
    /// The client needs to authenticate to gain network access.
    let [<Literal>] NetworkAuthenticationRequired = 511


type MultipartItem = | MultipartItem of formField: string * filename: string * content: Stream

/// The body to send in an HTTP request
type HttpRequestBody =
    | TextRequest of string
    | BinaryUpload of byte[]
    | FormValues of seq<string * string>
    /// A sequence of formParamName * fileName * fileContent groups
    | Multipart of boundary: string * parts: seq<MultipartItem>

/// The response body returned by an HTTP request
type HttpResponseBody =
    | Text of string
    | Binary of byte[]

/// The response returned by an HTTP request
type HttpResponse =
  { Body : HttpResponseBody
    StatusCode: int
    ResponseUrl : string
    /// If the same header is present multiple times, the values will be concatenated with comma as the separator
    Headers : Map<string,string>
    Cookies : Map<string,string> }

/// The response returned by an HTTP request with direct access to the response stream
type HttpResponseWithStream =
  { ResponseStream : Stream
    StatusCode: int
    ResponseUrl : string
    /// If the same header is present multiple times, the values will be concatenated with comma as the separator
    Headers : Map<string,string>
    Cookies : Map<string,string> }

/// Constants for common HTTP content types
module HttpContentTypes =
    /// */*
    let [<Literal>] Any = "*/*"
    /// plain/text
    let [<Literal>] Text = "text/plain"
    /// application/octet-stream
    let [<Literal>] Binary = "application/octet-stream"
    /// application/octet-stream
    let [<Literal>] Zip = "application/zip"
    /// application/octet-stream
    let [<Literal>] GZip = "application/gzip"
    /// application/x-www-form-urlencoded
    let [<Literal>] FormValues = "application/x-www-form-urlencoded"
    /// application/json
    let [<Literal>] Json = "application/json"
    /// application/javascript
    let [<Literal>] JavaScript = "application/javascript"
    /// application/xml
    let [<Literal>] Xml = "application/xml"
    /// application/rss+xml
    let [<Literal>] Rss = "application/rss+xml"
    /// application/atom+xml
    let [<Literal>] Atom = "application/atom+xml"
    /// application/rdf+xml
    let [<Literal>] Rdf = "application/rdf+xml"
    /// text/html
    let [<Literal>] Html = "text/html"
    /// application/xhtml+xml
    let [<Literal>] XHtml = "application/xhtml+xml"
    /// application/soap+xml
    let [<Literal>] Soap = "application/soap+xml"
    /// text/csv
    let [<Literal>] Csv = "text/csv"
    /// application/json-rpc
    let [<Literal>] JsonRpc = "application/json-rpc"    
    /// multipart/form-data
    let Multipart boundary = sprintf "multipart/form-data; boundary=%s" boundary

type private HeaderEnum = System.Net.HttpRequestHeader

module MimeTypes =
    open System.Collections.Generic
    let private pairs =
        [|
            (".323", "text/h323")
            (".3g2", "video/3gpp2")
            (".3gp", "video/3gpp")
            (".3gp2", "video/3gpp2")
            (".3gpp", "video/3gpp")
            (".7z", "application/x-7z-compressed")
            (".aa", "audio/audible")
            (".AAC", "audio/aac")
            (".aaf", "application/octet-stream")
            (".aax", "audio/vnd.audible.aax")
            (".ac3", "audio/ac3")
            (".aca", "application/octet-stream")
            (".accda", "application/msaccess.addin")
            (".accdb", "application/msaccess")
            (".accdc", "application/msaccess.cab")
            (".accde", "application/msaccess")
            (".accdr", "application/msaccess.runtime")
            (".accdt", "application/msaccess")
            (".accdw", "application/msaccess.webapplication")
            (".accft", "application/msaccess.ftemplate")
            (".acx", "application/internet-property-stream")
            (".AddIn", "text/xml")
            (".ade", "application/msaccess")
            (".adobebridge", "application/x-bridge-url")
            (".adp", "application/msaccess")
            (".ADT", "audio/vnd.dlna.adts")
            (".ADTS", "audio/aac")
            (".afm", "application/octet-stream")
            (".ai", "application/postscript")
            (".aif", "audio/aiff")
            (".aifc", "audio/aiff")
            (".aiff", "audio/aiff")
            (".air", "application/vnd.adobe.air-application-installer-package+zip")
            (".amc", "application/mpeg")
            (".anx", "application/annodex")
            (".apk", "application/vnd.android.package-archive" )
            (".application", "application/x-ms-application")
            (".art", "image/x-jg")
            (".asa", "application/xml")
            (".asax", "application/xml")
            (".ascx", "application/xml")
            (".asd", "application/octet-stream")
            (".asf", "video/x-ms-asf")
            (".ashx", "application/xml")
            (".asi", "application/octet-stream")
            (".asm", "text/plain")
            (".asmx", "application/xml")
            (".aspx", "application/xml")
            (".asr", "video/x-ms-asf")
            (".asx", "video/x-ms-asf")
            (".atom", "application/atom+xml")
            (".au", "audio/basic")
            (".avi", "video/x-msvideo")
            (".axa", "audio/annodex")
            (".axs", "application/olescript")
            (".axv", "video/annodex")
            (".bas", "text/plain")
            (".bcpio", "application/x-bcpio")
            (".bin", "application/octet-stream")
            (".bmp", "image/bmp")
            (".c", "text/plain")
            (".cab", "application/octet-stream")
            (".caf", "audio/x-caf")
            (".calx", "application/vnd.ms-office.calx")
            (".cat", "application/vnd.ms-pki.seccat")
            (".cc", "text/plain")
            (".cd", "text/plain")
            (".cdda", "audio/aiff")
            (".cdf", "application/x-cdf")
            (".cer", "application/x-x509-ca-cert")
            (".cfg", "text/plain")
            (".chm", "application/octet-stream")
            (".class", "application/x-java-applet")
            (".clp", "application/x-msclip")
            (".cmd", "text/plain")
            (".cmx", "image/x-cmx")
            (".cnf", "text/plain")
            (".cod", "image/cis-cod")
            (".config", "application/xml")
            (".contact", "text/x-ms-contact")
            (".coverage", "application/xml")
            (".cpio", "application/x-cpio")
            (".cpp", "text/plain")
            (".crd", "application/x-mscardfile")
            (".crl", "application/pkix-crl")
            (".crt", "application/x-x509-ca-cert")
            (".cs", "text/plain")
            (".csdproj", "text/plain")
            (".csh", "application/x-csh")
            (".csproj", "text/plain")
            (".css", "text/css")
            (".csv", "text/csv")
            (".cur", "application/octet-stream")
            (".cxx", "text/plain")
            (".dat", "application/octet-stream")
            (".datasource", "application/xml")
            (".dbproj", "text/plain")
            (".dcr", "application/x-director")
            (".def", "text/plain")
            (".deploy", "application/octet-stream")
            (".der", "application/x-x509-ca-cert")
            (".dgml", "application/xml")
            (".dib", "image/bmp")
            (".dif", "video/x-dv")
            (".dir", "application/x-director")
            (".disco", "text/xml")
            (".divx", "video/divx")
            (".dll", "application/x-msdownload")
            (".dll.config", "text/xml")
            (".dlm", "text/dlm")
            (".doc", "application/msword")
            (".docm", "application/vnd.ms-word.document.macroEnabled.12")
            (".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            (".dot", "application/msword")
            (".dotm", "application/vnd.ms-word.template.macroEnabled.12")
            (".dotx", "application/vnd.openxmlformats-officedocument.wordprocessingml.template")
            (".dsp", "application/octet-stream")
            (".dsw", "text/plain")
            (".dtd", "text/xml")
            (".dtsConfig", "text/xml")
            (".dv", "video/x-dv")
            (".dvi", "application/x-dvi")
            (".dwf", "drawing/x-dwf")
            (".dwp", "application/octet-stream")
            (".dxr", "application/x-director")
            (".eml", "message/rfc822")
            (".emz", "application/octet-stream")
            (".eot", "application/vnd.ms-fontobject")
            (".eps", "application/postscript")
            (".etl", "application/etl")
            (".etx", "text/x-setext")
            (".evy", "application/envoy")
            (".exe", "application/octet-stream")
            (".exe.config", "text/xml")
            (".fdf", "application/vnd.fdf")
            (".fif", "application/fractals")
            (".filters", "application/xml")
            (".fla", "application/octet-stream")
            (".flac", "audio/flac")
            (".flr", "x-world/x-vrml")
            (".flv", "video/x-flv")
            (".fsscript", "application/fsharp-script")
            (".fsx", "application/fsharp-script")
            (".generictest", "application/xml")
            (".gif", "image/gif")
            (".gpx", "application/gpx+xml")
            (".group", "text/x-ms-group")
            (".gsm", "audio/x-gsm")
            (".gtar", "application/x-gtar")
            (".gz", "application/x-gzip")
            (".h", "text/plain")
            (".hdf", "application/x-hdf")
            (".hdml", "text/x-hdml")
            (".hhc", "application/x-oleobject")
            (".hhk", "application/octet-stream")
            (".hhp", "application/octet-stream")
            (".hlp", "application/winhlp")
            (".hpp", "text/plain")
            (".hqx", "application/mac-binhex40")
            (".hta", "application/hta")
            (".htc", "text/x-component")
            (".htm", "text/html")
            (".html", "text/html")
            (".htt", "text/webviewhtml")
            (".hxa", "application/xml")
            (".hxc", "application/xml")
            (".hxd", "application/octet-stream")
            (".hxe", "application/xml")
            (".hxf", "application/xml")
            (".hxh", "application/octet-stream")
            (".hxi", "application/octet-stream")
            (".hxk", "application/xml")
            (".hxq", "application/octet-stream")
            (".hxr", "application/octet-stream")
            (".hxs", "application/octet-stream")
            (".hxt", "text/html")
            (".hxv", "application/xml")
            (".hxw", "application/octet-stream")
            (".hxx", "text/plain")
            (".i", "text/plain")
            (".ico", "image/x-icon")
            (".ics", "application/octet-stream")
            (".idl", "text/plain")
            (".ief", "image/ief")
            (".iii", "application/x-iphone")
            (".inc", "text/plain")
            (".inf", "application/octet-stream")
            (".ini", "text/plain")
            (".inl", "text/plain")
            (".ins", "application/x-internet-signup")
            (".ipa", "application/x-itunes-ipa")
            (".ipg", "application/x-itunes-ipg")
            (".ipproj", "text/plain")
            (".ipsw", "application/x-itunes-ipsw")
            (".iqy", "text/x-ms-iqy")
            (".isp", "application/x-internet-signup")
            (".ite", "application/x-itunes-ite")
            (".itlp", "application/x-itunes-itlp")
            (".itms", "application/x-itunes-itms")
            (".itpc", "application/x-itunes-itpc")
            (".IVF", "video/x-ivf")
            (".jar", "application/java-archive")
            (".java", "application/octet-stream")
            (".jck", "application/liquidmotion")
            (".jcz", "application/liquidmotion")
            (".jfif", "image/pjpeg")
            (".jnlp", "application/x-java-jnlp-file")
            (".jpb", "application/octet-stream")
            (".jpe", "image/jpeg")
            (".jpeg", "image/jpeg")
            (".jpg", "image/jpeg")
            (".js", "application/javascript")
            (".json", "application/json")
            (".jsx", "text/jscript")
            (".jsxbin", "text/plain")
            (".latex", "application/x-latex")
            (".library-ms", "application/windows-library+xml")
            (".lit", "application/x-ms-reader")
            (".loadtest", "application/xml")
            (".lpk", "application/octet-stream")
            (".lsf", "video/x-la-asf")
            (".lst", "text/plain")
            (".lsx", "video/x-la-asf")
            (".lzh", "application/octet-stream")
            (".m13", "application/x-msmediaview")
            (".m14", "application/x-msmediaview")
            (".m1v", "video/mpeg")
            (".m2t", "video/vnd.dlna.mpeg-tts")
            (".m2ts", "video/vnd.dlna.mpeg-tts")
            (".m2v", "video/mpeg")
            (".m3u", "audio/x-mpegurl")
            (".m3u8", "audio/x-mpegurl")
            (".m4a", "audio/m4a")
            (".m4b", "audio/m4b")
            (".m4p", "audio/m4p")
            (".m4r", "audio/x-m4r")
            (".m4v", "video/x-m4v")
            (".mac", "image/x-macpaint")
            (".mak", "text/plain")
            (".man", "application/x-troff-man")
            (".manifest", "application/x-ms-manifest")
            (".map", "text/plain")
            (".master", "application/xml")
            (".mda", "application/msaccess")
            (".mdb", "application/x-msaccess")
            (".mde", "application/msaccess")
            (".mdp", "application/octet-stream")
            (".me", "application/x-troff-me")
            (".mfp", "application/x-shockwave-flash")
            (".mht", "message/rfc822")
            (".mhtml", "message/rfc822")
            (".mid", "audio/mid")
            (".midi", "audio/mid")
            (".mix", "application/octet-stream")
            (".mk", "text/plain")
            (".mmf", "application/x-smaf")
            (".mno", "text/xml")
            (".mny", "application/x-msmoney")
            (".mod", "video/mpeg")
            (".mov", "video/quicktime")
            (".movie", "video/x-sgi-movie")
            (".mp2", "video/mpeg")
            (".mp2v", "video/mpeg")
            (".mp3", "audio/mpeg")
            (".mp4", "video/mp4")
            (".mp4v", "video/mp4")
            (".mpa", "video/mpeg")
            (".mpe", "video/mpeg")
            (".mpeg", "video/mpeg")
            (".mpf", "application/vnd.ms-mediapackage")
            (".mpg", "video/mpeg")
            (".mpp", "application/vnd.ms-project")
            (".mpv2", "video/mpeg")
            (".mqv", "video/quicktime")
            (".ms", "application/x-troff-ms")
            (".msi", "application/octet-stream")
            (".mso", "application/octet-stream")
            (".mts", "video/vnd.dlna.mpeg-tts")
            (".mtx", "application/xml")
            (".mvb", "application/x-msmediaview")
            (".mvc", "application/x-miva-compiled")
            (".mxp", "application/x-mmxp")
            (".nc", "application/x-netcdf")
            (".nsc", "video/x-ms-asf")
            (".nws", "message/rfc822")
            (".ocx", "application/octet-stream")
            (".oda", "application/oda")
            (".odb", "application/vnd.oasis.opendocument.database")
            (".odc", "application/vnd.oasis.opendocument.chart")
            (".odf", "application/vnd.oasis.opendocument.formula")
            (".odg", "application/vnd.oasis.opendocument.graphics")
            (".odh", "text/plain")
            (".odi", "application/vnd.oasis.opendocument.image")
            (".odl", "text/plain")
            (".odm", "application/vnd.oasis.opendocument.text-master")
            (".odp", "application/vnd.oasis.opendocument.presentation")
            (".ods", "application/vnd.oasis.opendocument.spreadsheet")
            (".odt", "application/vnd.oasis.opendocument.text")
            (".oga", "audio/ogg")
            (".ogg", "audio/ogg")
            (".ogv", "video/ogg")
            (".ogx", "application/ogg")
            (".one", "application/onenote")
            (".onea", "application/onenote")
            (".onepkg", "application/onenote")
            (".onetmp", "application/onenote")
            (".onetoc", "application/onenote")
            (".onetoc2", "application/onenote")
            (".opus", "audio/ogg")
            (".orderedtest", "application/xml")
            (".osdx", "application/opensearchdescription+xml")
            (".otf", "application/font-sfnt")
            (".otg", "application/vnd.oasis.opendocument.graphics-template")
            (".oth", "application/vnd.oasis.opendocument.text-web")
            (".otp", "application/vnd.oasis.opendocument.presentation-template")
            (".ots", "application/vnd.oasis.opendocument.spreadsheet-template")
            (".ott", "application/vnd.oasis.opendocument.text-template")
            (".oxt", "application/vnd.openofficeorg.extension")
            (".p10", "application/pkcs10")
            (".p12", "application/x-pkcs12")
            (".p7b", "application/x-pkcs7-certificates")
            (".p7c", "application/pkcs7-mime")
            (".p7m", "application/pkcs7-mime")
            (".p7r", "application/x-pkcs7-certreqresp")
            (".p7s", "application/pkcs7-signature")
            (".pbm", "image/x-portable-bitmap")
            (".pcast", "application/x-podcast")
            (".pct", "image/pict")
            (".pcx", "application/octet-stream")
            (".pcz", "application/octet-stream")
            (".pdf", "application/pdf")
            (".pfb", "application/octet-stream")
            (".pfm", "application/octet-stream")
            (".pfx", "application/x-pkcs12")
            (".pgm", "image/x-portable-graymap")
            (".pic", "image/pict")
            (".pict", "image/pict")
            (".pkgdef", "text/plain")
            (".pkgundef", "text/plain")
            (".pko", "application/vnd.ms-pki.pko")
            (".pls", "audio/scpls")
            (".pma", "application/x-perfmon")
            (".pmc", "application/x-perfmon")
            (".pml", "application/x-perfmon")
            (".pmr", "application/x-perfmon")
            (".pmw", "application/x-perfmon")
            (".png", "image/png")
            (".pnm", "image/x-portable-anymap")
            (".pnt", "image/x-macpaint")
            (".pntg", "image/x-macpaint")
            (".pnz", "image/png")
            (".pot", "application/vnd.ms-powerpoint")
            (".potm", "application/vnd.ms-powerpoint.template.macroEnabled.12")
            (".potx", "application/vnd.openxmlformats-officedocument.presentationml.template")
            (".ppa", "application/vnd.ms-powerpoint")
            (".ppam", "application/vnd.ms-powerpoint.addin.macroEnabled.12")
            (".ppm", "image/x-portable-pixmap")
            (".pps", "application/vnd.ms-powerpoint")
            (".ppsm", "application/vnd.ms-powerpoint.slideshow.macroEnabled.12")
            (".ppsx", "application/vnd.openxmlformats-officedocument.presentationml.slideshow")
            (".ppt", "application/vnd.ms-powerpoint")
            (".pptm", "application/vnd.ms-powerpoint.presentation.macroEnabled.12")
            (".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation")
            (".prf", "application/pics-rules")
            (".prm", "application/octet-stream")
            (".prx", "application/octet-stream")
            (".ps", "application/postscript")
            (".psc1", "application/PowerShell")
            (".psd", "application/octet-stream")
            (".psess", "application/xml")
            (".psm", "application/octet-stream")
            (".psp", "application/octet-stream")
            (".pub", "application/x-mspublisher")
            (".pwz", "application/vnd.ms-powerpoint")
            (".qht", "text/x-html-insertion")
            (".qhtm", "text/x-html-insertion")
            (".qt", "video/quicktime")
            (".qti", "image/x-quicktime")
            (".qtif", "image/x-quicktime")
            (".qtl", "application/x-quicktimeplayer")
            (".qxd", "application/octet-stream")
            (".ra", "audio/x-pn-realaudio")
            (".ram", "audio/x-pn-realaudio")
            (".rar", "application/x-rar-compressed")
            (".ras", "image/x-cmu-raster")
            (".rat", "application/rat-file")
            (".rc", "text/plain")
            (".rc2", "text/plain")
            (".rct", "text/plain")
            (".rdlc", "application/xml")
            (".reg", "text/plain")
            (".resx", "application/xml")
            (".rf", "image/vnd.rn-realflash")
            (".rgb", "image/x-rgb")
            (".rgs", "text/plain")
            (".rm", "application/vnd.rn-realmedia")
            (".rmi", "audio/mid")
            (".rmp", "application/vnd.rn-rn_music_package")
            (".roff", "application/x-troff")
            (".rpm", "audio/x-pn-realaudio-plugin")
            (".rqy", "text/x-ms-rqy")
            (".rtf", "application/rtf")
            (".rtx", "text/richtext")
            (".ruleset", "application/xml")
            (".s", "text/plain")
            (".safariextz", "application/x-safari-safariextz")
            (".scd", "application/x-msschedule")
            (".scr", "text/plain")
            (".sct", "text/scriptlet")
            (".sd2", "audio/x-sd2")
            (".sdp", "application/sdp")
            (".sea", "application/octet-stream")
            (".searchConnector-ms", "application/windows-search-connector+xml")
            (".setpay", "application/set-payment-initiation")
            (".setreg", "application/set-registration-initiation")
            (".settings", "application/xml")
            (".sgimb", "application/x-sgimb")
            (".sgml", "text/sgml")
            (".sh", "application/x-sh")
            (".shar", "application/x-shar")
            (".shtml", "text/html")
            (".sit", "application/x-stuffit")
            (".sitemap", "application/xml")
            (".skin", "application/xml")
            (".sldm", "application/vnd.ms-powerpoint.slide.macroEnabled.12")
            (".sldx", "application/vnd.openxmlformats-officedocument.presentationml.slide")
            (".slk", "application/vnd.ms-excel")
            (".sln", "text/plain")
            (".slupkg-ms", "application/x-ms-license")
            (".smd", "audio/x-smd")
            (".smi", "application/octet-stream")
            (".smx", "audio/x-smd")
            (".smz", "audio/x-smd")
            (".snd", "audio/basic")
            (".snippet", "application/xml")
            (".snp", "application/octet-stream")
            (".sol", "text/plain")
            (".sor", "text/plain")
            (".spc", "application/x-pkcs7-certificates")
            (".spl", "application/futuresplash")
            (".spx", "audio/ogg")
            (".src", "application/x-wais-source")
            (".srf", "text/plain")
            (".SSISDeploymentManifest", "text/xml")
            (".ssm", "application/streamingmedia")
            (".sst", "application/vnd.ms-pki.certstore")
            (".stl", "application/vnd.ms-pki.stl")
            (".sv4cpio", "application/x-sv4cpio")
            (".sv4crc", "application/x-sv4crc")
            (".svc", "application/xml")
            (".svg", "image/svg+xml")
            (".swf", "application/x-shockwave-flash")
            (".step", "application/step")
            (".stp", "application/step")
            (".t", "application/x-troff")
            (".tar", "application/x-tar")
            (".tcl", "application/x-tcl")
            (".testrunconfig", "application/xml")
            (".testsettings", "application/xml")
            (".tex", "application/x-tex")
            (".texi", "application/x-texinfo")
            (".texinfo", "application/x-texinfo")
            (".tgz", "application/x-compressed")
            (".thmx", "application/vnd.ms-officetheme")
            (".thn", "application/octet-stream")
            (".tif", "image/tiff")
            (".tiff", "image/tiff")
            (".tlh", "text/plain")
            (".tli", "text/plain")
            (".toc", "application/octet-stream")
            (".tr", "application/x-troff")
            (".trm", "application/x-msterminal")
            (".trx", "application/xml")
            (".ts", "video/vnd.dlna.mpeg-tts")
            (".tsv", "text/tab-separated-values")
            (".ttf", "application/font-sfnt")
            (".tts", "video/vnd.dlna.mpeg-tts")
            (".txt", "text/plain")
            (".u32", "application/octet-stream")
            (".uls", "text/iuls")
            (".user", "text/plain")
            (".ustar", "application/x-ustar")
            (".vb", "text/plain")
            (".vbdproj", "text/plain")
            (".vbk", "video/mpeg")
            (".vbproj", "text/plain")
            (".vbs", "text/vbscript")
            (".vcf", "text/x-vcard")
            (".vcproj", "application/xml")
            (".vcs", "text/plain")
            (".vcxproj", "application/xml")
            (".vddproj", "text/plain")
            (".vdp", "text/plain")
            (".vdproj", "text/plain")
            (".vdx", "application/vnd.ms-visio.viewer")
            (".vml", "text/xml")
            (".vscontent", "application/xml")
            (".vsct", "text/xml")
            (".vsd", "application/vnd.visio")
            (".vsi", "application/ms-vsi")
            (".vsix", "application/vsix")
            (".vsixlangpack", "text/xml")
            (".vsixmanifest", "text/xml")
            (".vsmdi", "application/xml")
            (".vspscc", "text/plain")
            (".vss", "application/vnd.visio")
            (".vsscc", "text/plain")
            (".vssettings", "text/xml")
            (".vssscc", "text/plain")
            (".vst", "application/vnd.visio")
            (".vstemplate", "text/xml")
            (".vsto", "application/x-ms-vsto")
            (".vsw", "application/vnd.visio")
            (".vsx", "application/vnd.visio")
            (".vtx", "application/vnd.visio")
            (".wav", "audio/wav")
            (".wave", "audio/wav")
            (".wax", "audio/x-ms-wax")
            (".wbk", "application/msword")
            (".wbmp", "image/vnd.wap.wbmp")
            (".wcm", "application/vnd.ms-works")
            (".wdb", "application/vnd.ms-works")
            (".wdp", "image/vnd.ms-photo")
            (".webarchive", "application/x-safari-webarchive")
            (".webm", "video/webm")
            (".webp", "image/webp")
            (".webtest", "application/xml")
            (".wiq", "application/xml")
            (".wiz", "application/msword")
            (".wks", "application/vnd.ms-works")
            (".WLMP", "application/wlmoviemaker")
            (".wlpginstall", "application/x-wlpg-detect")
            (".wlpginstall3", "application/x-wlpg3-detect")
            (".wm", "video/x-ms-wm")
            (".wma", "audio/x-ms-wma")
            (".wmd", "application/x-ms-wmd")
            (".wmf", "application/x-msmetafile")
            (".wml", "text/vnd.wap.wml")
            (".wmlc", "application/vnd.wap.wmlc")
            (".wmls", "text/vnd.wap.wmlscript")
            (".wmlsc", "application/vnd.wap.wmlscriptc")
            (".wmp", "video/x-ms-wmp")
            (".wmv", "video/x-ms-wmv")
            (".wmx", "video/x-ms-wmx")
            (".wmz", "application/x-ms-wmz")
            (".woff", "application/font-woff")
            (".wpl", "application/vnd.ms-wpl")
            (".wps", "application/vnd.ms-works")
            (".wri", "application/x-mswrite")
            (".wrl", "x-world/x-vrml")
            (".wrz", "x-world/x-vrml")
            (".wsc", "text/scriptlet")
            (".wsdl", "text/xml")
            (".wvx", "video/x-ms-wvx")
            (".x", "application/directx")
            (".xaf", "x-world/x-vrml")
            (".xaml", "application/xaml+xml")
            (".xap", "application/x-silverlight-app")
            (".xbap", "application/x-ms-xbap")
            (".xbm", "image/x-xbitmap")
            (".xdr", "text/plain")
            (".xht", "application/xhtml+xml")
            (".xhtml", "application/xhtml+xml")
            (".xla", "application/vnd.ms-excel")
            (".xlam", "application/vnd.ms-excel.addin.macroEnabled.12")
            (".xlc", "application/vnd.ms-excel")
            (".xld", "application/vnd.ms-excel")
            (".xlk", "application/vnd.ms-excel")
            (".xll", "application/vnd.ms-excel")
            (".xlm", "application/vnd.ms-excel")
            (".xls", "application/vnd.ms-excel")
            (".xlsb", "application/vnd.ms-excel.sheet.binary.macroEnabled.12")
            (".xlsm", "application/vnd.ms-excel.sheet.macroEnabled.12")
            (".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            (".xlt", "application/vnd.ms-excel")
            (".xltm", "application/vnd.ms-excel.template.macroEnabled.12")
            (".xltx", "application/vnd.openxmlformats-officedocument.spreadsheetml.template")
            (".xlw", "application/vnd.ms-excel")
            (".xml", "text/xml")
            (".xmta", "application/xml")
            (".xof", "x-world/x-vrml")
            (".XOML", "text/plain")
            (".xpm", "image/x-xpixmap")
            (".xps", "application/vnd.ms-xpsdocument")
            (".xrm-ms", "text/xml")
            (".xsc", "application/xml")
            (".xsd", "text/xml")
            (".xsf", "text/xml")
            (".xsl", "text/xml")
            (".xslt", "text/xml")
            (".xsn", "application/octet-stream")
            (".xss", "application/xml")
            (".xspf", "application/xspf+xml")
            (".xtp", "application/octet-stream")
            (".xwd", "image/x-xwindowdump")
            (".z", "application/x-compress")
            (".zip", "application/zip") |]

    let private map = Map.ofArray pairs
        
    let tryFind (ext: string) = Map.tryFind (ext.ToLowerInvariant()) map

/// Constants for common HTTP encodings
module HttpEncodings =

    /// ISO-8859-1
    let PostDefaultEncoding = Encoding.GetEncoding("ISO-8859-1") // http://stackoverflow.com/questions/708915/detecting-the-character-encoding-of-an-http-post-request/708942#708942

    /// ISO-8859-1
    let ResponseDefaultEncoding = Encoding.GetEncoding("ISO-8859-1") // http://www.ietf.org/rfc/rfc2616.txt

    let internal getEncoding (encodingStr:string) =
        match Int32.TryParse(encodingStr, NumberStyles.Integer, CultureInfo.InvariantCulture) with
        | true, codepage -> Encoding.GetEncoding codepage
        | _ -> Encoding.GetEncoding encodingStr

[<AutoOpen>]
module private HttpHelpers =

    /// Decorator for System.Net.WebResponse class
    /// used to make response stream seekable
    /// in order to preserve it in the new response
    type WebResponse(res : System.Net.WebResponse) =
        inherit System.Net.WebResponse()

        let copyToMemoryStream (inputStream : Stream) =
            let bufferLen : int = 4096
            let buffer : byte array = Array.zeroCreate bufferLen

            let outputStream = new MemoryStream()
            let rec copy () =
                match inputStream.Read(buffer, 0, bufferLen) with
                | count when count > 0 ->
                    outputStream.Write(buffer, 0, count)
                    copy ()
                | _ -> ()
            copy ()
            outputStream.Position <- 0L
            outputStream

        let responseStream = res.GetResponseStream() |> copyToMemoryStream

        override x.Headers = res.Headers
        override x.ResponseUri = res.ResponseUri
        override x.ContentType = res.ContentType
        override x.ContentLength = responseStream.Length
        override x.SupportsHeaders = res.SupportsHeaders
        override x.IsFromCache = res.IsFromCache
        override x.IsMutuallyAuthenticated = res.IsMutuallyAuthenticated
        override x.Close () = res.Close()
        override x.GetResponseStream () = responseStream :> Stream
        member x.ResetResponseStream () = responseStream.Position <- 0L

        interface IDisposable with
            member x.Dispose () =
                match res :> obj with
                | :? IDisposable as res -> res.Dispose ()
                | _ -> ()
                responseStream.Dispose ()

    /// consumes a stream asynchronously until the end
    /// and returns a memory stream with the full content
    let asyncRead (stream:Stream) = async {
        use stream = stream
        let output = new MemoryStream ()
        do! stream.CopyToAsync(output) |> Async.AwaitIAsyncResult |> Async.Ignore
        output.Seek(0L, SeekOrigin.Begin) |> ignore
        return output
    }

    /// A stream class that abstracts away writing the contents of a series of other streams, closing them as they are consumed.  Non-seekable, reading-only stream.
    type CombinedStream(length, streams: Stream seq) =
        inherit Stream() with
            let mutable v = 0L
            let mutable streams = streams |> Seq.cache

            let rec readFromStream buffer offset count =
                if Seq.isEmpty streams
                then 0
                else
                    let stream = Seq.head streams
                    let read = stream.Read(buffer, offset, min count (int stream.Length))
                    if read < count
                    then
                        stream.Dispose()
                        streams <- streams |> Seq.skip 1
                        let readFromRest = readFromStream buffer (offset + read) (count - read)
                        read + readFromRest
                    else read

            override x.CanRead = true
            override x.CanSeek = false
            override x.CanWrite = false
            override x.Length with get () = length
            override x.Position with get () = v and set(_) = failwith "no position setting"
            override x.Flush() = ()
            override x.CanTimeout = false
            override x.Seek(_,_) = failwith "no seeking"
            override x.SetLength(_) = failwith "no setting length"
            override x.Write(_,_,_) = failwith "no writing"
            override x.WriteByte(_) = failwith "seriously, no writing"
            override x.Read(buffer, offset, count) = readFromStream buffer offset count
            interface IDisposable with
                member x.Dispose() = streams |> Seq.iter (fun s -> s.Dispose()) |> ignore

    ///     1) compute length (parts.Length * boundary_size) + Sum(parts.Streams.Length)
    ///     2) foreach part (formFieldName, fileName, fileContent)
    ///         a) write initial boundary marker
    ///         b) write section headers (start with content-type/Content-Disposition based on the extension of the second parameter, plus name and fileName)
    ///         c) write newline
    ///         d) write section data
    ///     3) write trailing boundary
    let writeMultipart (boundary: string) (parts: seq<MultipartItem>) (e : Encoding) =
        let newlineStream () = new MemoryStream(e.GetBytes "\r\n") :> Stream
        let prefixedBoundary = sprintf "--%s" boundary
        let segments = parts |> Seq.map (fun (MultipartItem(formField, fileName, fileStream)) ->
            let fileExt = Path.GetExtension fileName    
            let contentType = defaultArg (MimeTypes.tryFind fileExt) "application/octet-stream"
            let printHeader (header, value) = sprintf "%s: %s" header value
            let headerpart =
                [ prefixedBoundary
                  HttpRequestHeaders.ContentDisposition("form-data", Some formField, Some fileName) |> printHeader
                  HttpRequestHeaders.ContentType contentType |> printHeader ]
                |> String.concat Environment.NewLine
            let headerStream =
                let bytes = e.GetBytes headerpart
                new MemoryStream(bytes) :> Stream
            let partSubstreams =
                [ headerStream
                  newlineStream()
                  newlineStream()
                  fileStream ]
            let partLength = partSubstreams |> Seq.sumBy (fun s -> s.Length)
            new CombinedStream(partLength, partSubstreams) :> Stream
        )

        /// per spec, the end boundary is the given boundary with a trailing --
        let endBoundaryStream =
            let text = sprintf "%s--" prefixedBoundary
            let bytes = e.GetBytes text
            new MemoryStream(bytes) :> Stream

        let wholePayload = Seq.append segments [newlineStream(); endBoundaryStream; ]
        let wholePayloadLength = wholePayload |> Seq.sumBy (fun s -> s.Length)
        new CombinedStream(wholePayloadLength, wholePayload) :> Stream

    let asyncCopy (source: Stream) (dest: Stream) =
        async {
            do! source.CopyToAsync(dest) |> Async.AwaitIAsyncResult |> Async.Ignore
            source.Dispose ()
        }

    let runningOnMono = try System.Type.GetType("Mono.Runtime") <> null with e -> false 

    let writeBody (req:HttpWebRequest) (data: Stream) =
        async {
            req.ContentLength <- data.Length
            use! output = req.GetRequestStreamAsync () |> Async.AwaitTask
            do! asyncCopy data output
            output.Flush()
        }

    let reraisePreserveStackTrace (e:Exception) =
        try
            let remoteStackTraceString = typeof<exn>.GetField("_remoteStackTraceString", BindingFlags.Instance ||| BindingFlags.NonPublic);
            if remoteStackTraceString <> null then
                remoteStackTraceString.SetValue(e, e.StackTrace + Environment.NewLine)
        with _ -> ()
        raise e

    let augmentWebExceptionsWithDetails f = async {
        try
            return! f()
        with
            // If an exception happens, augment the message with the response
            | :? WebException as exn ->
              if exn.Response = null then reraisePreserveStackTrace exn
              let responseExn =
                  try
                    let newResponse = new WebResponse(exn.Response)
                    let responseStream = newResponse.GetResponseStream()
                    let streamReader = new StreamReader(responseStream)
                    let responseText = streamReader.ReadToEnd()
                    newResponse.ResetResponseStream ()
                    if String.IsNullOrEmpty responseText then None
                    else Some(WebException(sprintf "%s\nResponse from %s:\n%s" exn.Message newResponse.ResponseUri.OriginalString responseText, exn, exn.Status, newResponse))
                  with _ -> None
              match responseExn with
              | Some e -> raise e
              | None -> reraisePreserveStackTrace exn
              // just to keep the type-checker happy:
              return Unchecked.defaultof<_>
    }

    let rec checkForRepeatedHeaders visitedHeaders remainingHeaders =
        match remainingHeaders with
        | [] -> ()
        | header::remainingHeaders ->
            for visitedHeader in visitedHeaders do
                let name1, name2 = fst header, fst visitedHeader
                if name1 = name2 then failwithf "Repeated headers: %A %A" visitedHeader header
            checkForRepeatedHeaders (header::visitedHeaders) remainingHeaders

    let setHeaders headers (req:HttpWebRequest) =
        let hasContentType = ref false
        checkForRepeatedHeaders [] headers
        headers |> List.iter (fun (header:string, value) ->
            match header.ToLowerInvariant() with
            | "accept" -> req.Accept <- value
            | "accept-charset" -> req.Headers.[HeaderEnum.AcceptCharset] <- value
            | "accept-datetime" -> req.Headers.["Accept-Datetime"] <- value
            | "accept-encoding" -> req.Headers.[HeaderEnum.AcceptEncoding] <- value
            | "accept-language" -> req.Headers.[HeaderEnum.AcceptLanguage] <- value
            | "allow" -> req.Headers.[HeaderEnum.Allow] <- value
            | "authorization" -> req.Headers.[HeaderEnum.Authorization] <- value
            | "cache-control" -> req.Headers.[HeaderEnum.CacheControl] <- value
            | "connection" -> req.Connection <- value
            | "content-encoding" -> req.Headers.[HeaderEnum.ContentEncoding] <- value
            | "content-Language" -> req.Headers.[HeaderEnum.ContentLanguage] <- value
            | "content-Location" -> req.Headers.[HeaderEnum.ContentLocation] <- value
            | "content-md5" -> req.Headers.[HeaderEnum.ContentMd5] <- value
            | "content-range" -> req.Headers.[HeaderEnum.ContentRange] <- value
            | "content-type" ->
                req.ContentType <- value
                hasContentType := true
            | "date" -> req.Date <- DateTime.SpecifyKind(DateTime.ParseExact(value, "R", CultureInfo.InvariantCulture), DateTimeKind.Utc)
            | "expect" -> req.Expect <- value
            | "expires" -> req.Headers.[HeaderEnum.Expires] <- value
            | "from" -> req.Headers.[HeaderEnum.From] <- value
            | "host" -> req.Host <- value
            | "if-match" -> req.Headers.[HeaderEnum.IfMatch] <- value
            | "if-modified-since" -> req.IfModifiedSince <- DateTime.SpecifyKind(DateTime.ParseExact(value, "R", CultureInfo.InvariantCulture), DateTimeKind.Utc)
            | "if-none-match" -> req.Headers.[HeaderEnum.IfNoneMatch] <- value
            | "if-range" -> req.Headers.[HeaderEnum.IfRange] <- value
            | "if-unmodified-since" -> req.Headers.[HeaderEnum.IfUnmodifiedSince] <- value
            | "keep-alive" -> req.Headers.[HeaderEnum.KeepAlive] <- value
            | "last-modified" -> req.Headers.[HeaderEnum.LastModified] <- value
            | "max-forwards" -> req.Headers.[HeaderEnum.MaxForwards] <- value
            | "origin" -> req.Headers.["Origin"] <- value
            | "pragma" -> req.Headers.[HeaderEnum.Pragma] <- value
            | "range" ->
                if not (value.StartsWith("bytes=")) then failwith "Invalid value for the Range header"
                let bytes = value.Substring("bytes=".Length).Split('-')
                if bytes.Length <> 2 then failwith "Invalid value for the Range header"
                req.AddRange(int64 bytes.[0], int64 bytes.[1])
            | "proxy-authorization" -> req.Headers.[HeaderEnum.ProxyAuthorization] <- value
            | "referer" -> req.Referer <- value
            | "te" -> req.Headers.[HeaderEnum.Te] <- value
            | "trailer" -> req.Headers.[HeaderEnum.Trailer] <- value
            | "translate" -> req.Headers.[HeaderEnum.Translate] <- value
            | "upgrade" -> req.Headers.[HeaderEnum.Upgrade] <- value
            | "user-agent" -> req.UserAgent <- value
            | "via" -> req.Headers.[HeaderEnum.Via] <- value
            | "warning" -> req.Headers.[HeaderEnum.Warning] <- value
            | _ -> req.Headers.[header] <- value
        )
        hasContentType.Value

    let getResponse (req:HttpWebRequest) silentHttpErrors =

        let getResponseFromBeginEnd =
            Async.FromBeginEnd(req.BeginGetResponse, req.EndGetResponse)

        let getResponseAsync (req:HttpWebRequest) =
            if req.Timeout = Timeout.Infinite
                then getResponseFromBeginEnd
                else
                    async {
                        let! child = Async.StartChild(getResponseFromBeginEnd, req.Timeout)
                        try
                            return! child
                        with
                        | :? TimeoutException as exc ->
                            req.Abort()
                            raise <| WebException("Timeout exceeded while getting response", exc, WebExceptionStatus.Timeout, null)
                            return Unchecked.defaultof<_>
                    }

        if defaultArg silentHttpErrors false
            then
                async {
                    try
                        return! getResponseAsync req
                    with
                        | :? WebException as exc ->
                            if exc.Response <> null then
                               return exc.Response
                            else
                                reraisePreserveStackTrace exc
                                return Unchecked.defaultof<_>
                }
            else getResponseAsync req

    let toHttpResponse forceText responseUrl statusCode contentType
                       characterSet responseEncodingOverride cookies headers stream = async {

        let isText (mimeType:string) =
            let isText (mimeType:string) =
                let mimeType = mimeType.Trim()
                mimeType.StartsWith "text/" ||
                mimeType = HttpContentTypes.Json ||
                mimeType = HttpContentTypes.Xml ||
                mimeType = HttpContentTypes.JavaScript ||
                mimeType = HttpContentTypes.JsonRpc ||
                mimeType = "application/ecmascript" ||
                mimeType = "application/xml-dtd" ||
                mimeType.StartsWith "application/" && mimeType.EndsWith "+xml" ||
                mimeType.StartsWith "application/" && mimeType.EndsWith "+json"
            mimeType.Split([| ';' |], StringSplitOptions.RemoveEmptyEntries)
            |> Array.exists isText

        let! memoryStream = asyncRead stream

        let respBody =
            if forceText || isText contentType then
                let encoding =
                    match (defaultArg responseEncodingOverride ""), characterSet with
                    | "", "" -> HttpEncodings.ResponseDefaultEncoding
                    // some web servers respond with broken things like Content-Type: text/xml; charset="UTF-8"
                    // this goes against rfc2616, but it breaks Encoding.GetEncoding, so let us strip this char out
                    | "", characterSet -> Encoding.GetEncoding (characterSet.Replace("\"",""))
                    | responseEncodingOverride, _ -> HttpEncodings.getEncoding responseEncodingOverride
                use sr = new StreamReader(memoryStream, encoding)
                sr.ReadToEnd() |> Text
            else
                memoryStream.ToArray() |> Binary

        return { Body = respBody
                 StatusCode = statusCode
                 ResponseUrl = responseUrl
                 Headers = headers
                 Cookies = cookies }
    }

module internal CookieHandling =

    // .NET has trouble parsing some cookies. See http://stackoverflow.com/a/22098131/165633
    let getAllCookiesFromHeader (header:string) (responseUri:Uri) =

        let cookiesWithWrongSplit = header.Replace("\r", "").Replace("\n", "").Split(',')

        let isInvalidCookie (cookieStr:string) =
            let equalsPos = cookieStr.IndexOf '='
            equalsPos = -1
            ||
                let semicolonPos = cookieStr.IndexOf ';'
                semicolonPos <> -1 && semicolonPos < equalsPos

        let cookies = ResizeArray()
        let mutable i = 0
        while i < cookiesWithWrongSplit.Length do
            // the next one is not a new cookie but part of the current one
            let mutable currentCookie = cookiesWithWrongSplit.[i]
            while i < cookiesWithWrongSplit.Length - 1 && isInvalidCookie cookiesWithWrongSplit.[i + 1] do
                currentCookie <- currentCookie + "," + cookiesWithWrongSplit.[i + 1]
                i <- i + 1
            cookies.Add(currentCookie)
            i <- i + 1

        let inline startsWithIgnoreCase prefix (str:string) = str.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
        let inline equalsIgnoreCase other (str:string) = str.Equals(other, StringComparison.OrdinalIgnoreCase)
        let stripPrefix prefix str =
            if startsWithIgnoreCase prefix str
            then str.Substring(prefix.Length)
            else str
        let createCookie (cookieParts:string[]) =
            let cookie = Cookie()
            cookieParts |> Array.iteri (fun i cookiePart ->
                let cookiePart = cookiePart.Trim()
                if i = 0 then
                    let firstEqual = cookiePart.IndexOf '='
                    if firstEqual > -1 then
                        cookie.Name <- cookiePart.Substring(0, firstEqual)
                        cookie.Value <- cookiePart.Substring(firstEqual + 1)
                    else
                        cookie.Name <- cookiePart                    
                elif cookiePart |> startsWithIgnoreCase "path" then
                    let kvp = cookiePart.Split '='
                    if kvp.Length > 1 && kvp.[1] <> "" && kvp.[1] <> "/" then
                        cookie.Path <- kvp.[1]
                elif cookiePart |> startsWithIgnoreCase "domain" then
                    let kvp = cookiePart.Split '='
                    if kvp.Length > 1 then
                        let domain = 
                            kvp.[1] 
                            // remove spurious domain prefixes
                            |> stripPrefix "http://"
                            |> stripPrefix "https://"
                        if domain <> "" then
                            cookie.Domain <- domain
                elif cookiePart |> equalsIgnoreCase "secure" then
                    cookie.Secure <- true
                elif cookiePart |> equalsIgnoreCase "httponly" then
                    cookie.HttpOnly <- true
            )
            cookie
        [| for cookieStr in cookies do
            let cookieParts = cookieStr.Split([|';'|],StringSplitOptions.RemoveEmptyEntries)
            if cookieParts.Length > 0 then
                let cookie = createCookie cookieParts
                if cookie.Domain = "" then
                    cookie.Domain <- responseUri.Host
                let uriString = (if cookie.Secure then "https://" else "http://") + cookie.Domain.TrimStart('.') + cookie.Path
                match Uri.TryCreate(uriString, UriKind.Absolute) with
                | true, uri -> yield uri, cookie
                | _ -> ()
        |]

    let getCookiesAndManageCookieContainer uri responseUri (headers:Map<string, string>) (cookieContainer:CookieContainer) addCookiesToCookieContainer silentCookieErrors =
        let cookiesFromCookieContainer =
            cookieContainer.GetCookies uri
            |> Seq.cast<Cookie>
            |> Seq.map (fun cookie -> cookie.Name, cookie.Value)
            |> Map.ofSeq

        match headers.TryFind HttpResponseHeaders.SetCookie with
        | Some cookieHeader ->
            getAllCookiesFromHeader cookieHeader responseUri
            |> Array.fold (fun cookies (uri, cookie) ->
                if addCookiesToCookieContainer then
                    if silentCookieErrors then
                        try cookieContainer.Add(uri, cookie)
                        with :? CookieException -> ()
                    else
                        cookieContainer.Add(uri, cookie)
                cookies |> Map.add cookie.Name cookie.Value) cookiesFromCookieContainer
        | None -> cookiesFromCookieContainer

/// Utilities for working with network via HTTP. Includes methods for downloading
/// resources with specified headers, query parameters and HTTP body
[<AbstractClass>]
type Http private() =

    static let charsetRegex = Regex("charset=([^;\s]*)", RegexOptions.Compiled)
    
    /// Correctly encodes large form data values.
    /// See https://blogs.msdn.microsoft.com/yangxind/2006/11/08/dont-use-net-system-uri-unescapedatastring-in-url-decoding/
    /// and https://msdn.microsoft.com/en-us/library/system.uri.escapedatastring(v=vs.110).aspx
    static member internal EncodeFormData (query:string) =
        (WebUtility.UrlEncode query).Replace("+","%20")

    // EscapeUriString doesn't encode the & and # characters which cause issues, but EscapeDataString encodes too much making the url hard to read
    // So we use EscapeUriString and manually replace the two problematic characters
    static member private EncodeUrlParam (param: string) = 
        (Uri.EscapeUriString param).Replace("&", "%26").Replace("#", "%23")

    /// Appends the query parameters to the url, taking care of proper escaping
    static member internal AppendQueryToUrl(url:string, query) =
        match query with
        | [] -> url
        | query ->
            url
            + if url.Contains "?" then "&" else "?"
            + String.concat "&" [ for k, v in query -> Http.EncodeUrlParam k + "=" + Http.EncodeUrlParam v ]

    static member private InnerRequest
            (
                url:string,
                toHttpResponse,
                [<Optional>] ?query,
                [<Optional>] ?headers:seq<_>,
                [<Optional>] ?httpMethod,
                [<Optional>] ?body,
                [<Optional>] ?cookies:seq<_>,
                [<Optional>] ?cookieContainer,
                [<Optional>] ?silentHttpErrors,
                [<Optional>] ?silentCookieErrors,
                [<Optional>] ?responseEncodingOverride,
                [<Optional>] ?customizeHttpRequest,
                [<Optional>] ?timeout
            ) =

        let uri = Http.AppendQueryToUrl(url, defaultArg query []) |> Uri

        let req = WebRequest.CreateHttp uri

        // set method
        let defaultMethod = if body.IsSome then HttpMethod.Post else HttpMethod.Get
        req.Method <- (defaultArg httpMethod defaultMethod).ToString()

        // set headers
        let headers = defaultArg (Option.map List.ofSeq headers) []
        let hasContentType = setHeaders headers req

        req.AutomaticDecompression <- DecompressionMethods.GZip ||| DecompressionMethods.Deflate

        // set cookies
        let addCookiesFromHeadersToCookieContainer, cookieContainer =
            match cookieContainer with
            | Some x -> false, x
            | None -> true, CookieContainer()

        match cookies with
        | None -> ()
        | Some cookies -> cookies |> List.ofSeq |> List.iter (fun (name, value) -> cookieContainer.Add(req.RequestUri, Cookie(name, value)))

        req.CookieContainer <- cookieContainer        

        let getEncoding contentType =
            let charset = charsetRegex.Match(contentType)
            if charset.Success then
                Encoding.GetEncoding charset.Groups.[1].Value
            else
                HttpEncodings.PostDefaultEncoding

        let body = body |> Option.map (fun body ->

            let defaultContentType, (bytes: Encoding -> Stream) =
                match body with
                | TextRequest text -> HttpContentTypes.Text, (fun e -> new MemoryStream(e.GetBytes(text)) :> _)
                | BinaryUpload bytes -> HttpContentTypes.Binary, (fun _ -> new MemoryStream(bytes) :> _)
                | FormValues values ->
                    let bytes (e:Encoding) =
                        [ for k, v in values -> Http.EncodeFormData k + "=" + Http.EncodeFormData v ]
                        |> String.concat "&"
                        |> e.GetBytes
                    HttpContentTypes.FormValues, (fun e -> new MemoryStream(bytes e) :> _)
                | Multipart (boundary, parts) -> HttpContentTypes.Multipart(boundary), writeMultipart boundary parts

            // Set default content type if it is not specified by the user
            let encoding =
                if not hasContentType then
                    req.ContentType <- defaultContentType

                getEncoding req.ContentType

            bytes encoding)

        match timeout with
        | Some timeout -> req.Timeout <- timeout
        | None -> ()

        // Send the request and get the response
        augmentWebExceptionsWithDetails <| fun () -> async {

            let req =
                match customizeHttpRequest with
                | Some customizeHttpRequest -> customizeHttpRequest req
                | None -> req

            match body with
            | Some body -> do! writeBody req body
            | None -> ()

            let! resp = getResponse req silentHttpErrors

            let headers =
                [ for header in resp.Headers.AllKeys do
                    yield header, resp.Headers.[header] ]
                |> Map.ofList

            let cookies = CookieHandling.getCookiesAndManageCookieContainer uri resp.ResponseUri headers cookieContainer
                                                                            addCookiesFromHeadersToCookieContainer (defaultArg silentCookieErrors false)

            let contentType = if resp.ContentType = null then "application/octet-stream" else resp.ContentType

            let statusCode, characterSet =
                match resp with
                | :? HttpWebResponse as resp -> int resp.StatusCode, resp.CharacterSet
                | _ -> 0, ""

            let characterSet = if characterSet = null then "" else characterSet

            let stream = resp.GetResponseStream()

            return! toHttpResponse resp.ResponseUri.OriginalString statusCode contentType characterSet responseEncodingOverride cookies headers stream
        }

    /// Download an HTTP web resource from the specified URL asynchronously
    /// (allows specifying query string parameters and HTTP headers including
    /// headers that have to be handled specially - such as Accept, Content-Type & Referer)
    /// The body for POST request can be specified either as text or as a list of parameters
    /// that will be encoded, and the method will automatically be set if not specified
    static member AsyncRequest
            (
                url,
                [<Optional>] ?query,
                [<Optional>] ?headers,
                [<Optional>] ?httpMethod,
                [<Optional>] ?body,
                [<Optional>] ?cookies,
                [<Optional>] ?cookieContainer,
                [<Optional>] ?silentHttpErrors,
                [<Optional>] ?silentCookieErrors,
                [<Optional>] ?responseEncodingOverride,
                [<Optional>] ?customizeHttpRequest,
                [<Optional>] ?timeout
            ) =
        Http.InnerRequest(url, toHttpResponse (*forceText*)false, ?query=query, ?headers=headers, ?httpMethod=httpMethod, ?body=body, ?cookies=cookies, ?cookieContainer=cookieContainer, ?silentCookieErrors=silentCookieErrors,
                          ?silentHttpErrors=silentHttpErrors, ?responseEncodingOverride=responseEncodingOverride, ?customizeHttpRequest=customizeHttpRequest, ?timeout = timeout)

    /// Download an HTTP web resource from the specified URL asynchronously
    /// (allows specifying query string parameters and HTTP headers including
    /// headers that have to be handled specially - such as Accept, Content-Type & Referer)
    /// The body for POST request can be specified either as text or as a list of parameters
    /// that will be encoded, and the method will automatically be set if not specified
    static member AsyncRequestString
            (
                url,
                [<Optional>] ?query,
                [<Optional>] ?headers,
                [<Optional>] ?httpMethod,
                [<Optional>] ?body,
                [<Optional>] ?cookies,
                [<Optional>] ?cookieContainer,
                [<Optional>] ?silentHttpErrors,
                [<Optional>] ?silentCookieErrors,
                [<Optional>] ?responseEncodingOverride,
                [<Optional>] ?customizeHttpRequest,
                [<Optional>] ?timeout
            ) =
        async {
            let! response = Http.InnerRequest(url, toHttpResponse (*forceText*)true, ?query=query, ?headers=headers, ?httpMethod=httpMethod, ?body=body, ?cookies=cookies, ?cookieContainer=cookieContainer, ?silentCookieErrors = silentCookieErrors,
                                              ?silentHttpErrors=silentHttpErrors, ?responseEncodingOverride=responseEncodingOverride, ?customizeHttpRequest=customizeHttpRequest, ?timeout = timeout)
            return
                match response.Body with
                | Text text -> text
                | Binary binary -> failwithf "Expecting text, but got a binary response (%d bytes)" binary.Length
        }

    /// Download an HTTP web resource from the specified URL asynchronously
    /// (allows specifying query string parameters and HTTP headers including
    /// headers that have to be handled specially - such as Accept, Content-Type & Referer)
    /// The body for POST request can be specified either as text or as a list of parameters
    /// that will be encoded, and the method will automatically be set if not specified
    static member AsyncRequestStream
            (
                url,
                [<Optional>] ?query,
                [<Optional>] ?headers,
                [<Optional>] ?httpMethod,
                [<Optional>] ?body,
                [<Optional>] ?cookies,
                [<Optional>] ?cookieContainer,
                [<Optional>] ?silentHttpErrors,
                [<Optional>] ?silentCookieErrors,
                [<Optional>] ?customizeHttpRequest,
                [<Optional>] ?timeout
            ) =
        let toHttpResponse responseUrl statusCode _contentType _characterSet _responseEncodingOverride cookies headers stream = async {
            return { ResponseStream = stream
                     StatusCode = statusCode
                     ResponseUrl = responseUrl
                     Headers = headers
                     Cookies = cookies }
        }
        Http.InnerRequest(url, toHttpResponse, ?query=query, ?headers=headers, ?httpMethod=httpMethod, ?body=body, ?cookies=cookies, ?cookieContainer=cookieContainer, ?silentCookieErrors=silentCookieErrors,
                          ?silentHttpErrors=silentHttpErrors, ?customizeHttpRequest=customizeHttpRequest, ?timeout = timeout)

    /// Download an HTTP web resource from the specified URL synchronously
    /// (allows specifying query string parameters and HTTP headers including
    /// headers that have to be handled specially - such as Accept, Content-Type & Referer)
    /// The body for POST request can be specified either as text or as a list of parameters
    /// that will be encoded, and the method will automatically be set if not specified
    static member Request
            (
                url,
                [<Optional>] ?query,
                [<Optional>] ?headers,
                [<Optional>] ?httpMethod,
                [<Optional>] ?body,
                [<Optional>] ?cookies,
                [<Optional>] ?cookieContainer,
                [<Optional>] ?silentHttpErrors,
                [<Optional>] ?silentCookieErrors,
                [<Optional>] ?responseEncodingOverride,
                [<Optional>] ?customizeHttpRequest,
                [<Optional>] ?timeout
            ) =
        Http.AsyncRequest(url, ?query=query, ?headers=headers, ?httpMethod=httpMethod, ?body=body, ?cookies=cookies, ?cookieContainer=cookieContainer,?silentCookieErrors=silentCookieErrors,
                          ?silentHttpErrors=silentHttpErrors, ?responseEncodingOverride=responseEncodingOverride, ?customizeHttpRequest=customizeHttpRequest, ?timeout=timeout)
        |> Async.RunSynchronously

    /// Download an HTTP web resource from the specified URL synchronously
    /// (allows specifying query string parameters and HTTP headers including
    /// headers that have to be handled specially - such as Accept, Content-Type & Referer)
    /// The body for POST request can be specified either as text or as a list of parameters
    /// that will be encoded, and the method will automatically be set if not specified
    static member RequestString
            (
                url,
                [<Optional>] ?query,
                [<Optional>] ?headers,
                [<Optional>] ?httpMethod,
                [<Optional>] ?body,
                [<Optional>] ?cookies,
                [<Optional>] ?cookieContainer,
                [<Optional>] ?silentHttpErrors,
                [<Optional>] ?silentCookieErrors,
                [<Optional>] ?responseEncodingOverride,
                [<Optional>] ?customizeHttpRequest,
                [<Optional>] ?timeout
            ) =
        Http.AsyncRequestString(url, ?query=query, ?headers=headers, ?httpMethod=httpMethod, ?body=body, ?cookies=cookies, ?cookieContainer=cookieContainer, ?silentCookieErrors=silentCookieErrors,
                                ?silentHttpErrors=silentHttpErrors, ?responseEncodingOverride=responseEncodingOverride, ?customizeHttpRequest=customizeHttpRequest, ?timeout=timeout)
        |> Async.RunSynchronously

    /// Download an HTTP web resource from the specified URL synchronously
    /// (allows specifying query string parameters and HTTP headers including
    /// headers that have to be handled specially - such as Accept, Content-Type & Referer)
    /// The body for POST request can be specified either as text or as a list of parameters
    /// that will be encoded, and the method will automatically be set if not specified
    static member RequestStream
            (
                url,
                [<Optional>] ?query,
                [<Optional>] ?headers,
                [<Optional>] ?httpMethod,
                [<Optional>] ?body,
                [<Optional>] ?cookies,
                [<Optional>] ?cookieContainer,
                [<Optional>] ?silentHttpErrors,
                [<Optional>] ?silentCookieErrors,
                [<Optional>] ?customizeHttpRequest,
                [<Optional>] ?timeout
            ) =
        Http.AsyncRequestStream(url, ?query=query, ?headers=headers, ?httpMethod=httpMethod, ?body=body, ?cookies=cookies, ?cookieContainer=cookieContainer, ?silentCookieErrors=silentCookieErrors,
                                ?silentHttpErrors=silentHttpErrors, ?customizeHttpRequest=customizeHttpRequest, ?timeout=timeout)
        |> Async.RunSynchronously
