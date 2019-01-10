<?php
declare(strict_types = 1);

namespace Wayfair\Tremor;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ClientException as GuzzleClientException;
use GuzzleHttp\Stream\StreamInterface;
use MessagePack\Packer;
use Ramsey\Uuid\Uuid;
use Wayfair\Tremor\Exceptions\ClientException;
use Wayfair\Tremor\Exceptions\TremorException;
use Wayfair\Tremor\Proto;

/**
 * Class Client
 *
 * Tremor client for submitting events to tremot.
 *
 * @package Wayfair\Tremor
 */
class Client {
    /**
     * HTTP client isntance to talk to Tremor
     */
    private $client;

    /**
     * Hostname of the Tremor instnace
     *
     * @var string
     */
    private $host;

    /**
     * Body encoding to be used
     *
     * @var string
     */
    private $body_encoding = 'json';

    /**
     * Event, or outer wrapper encoding to be used
     *
     * @var string
     */
    private $event_encoding = 'json';

    /**
     * Packer instance for msgpack encoding
     *
     * @var Packer
     */
    private $packer;

    /**
     * Create a new Tremor Client Instance
     *
     * @param GuzzleClient $client         client setup witht the URL of the tremor host to send events to.
     * @param array        $opts           {
     *                                     Optional. Options to be passed to control the behaviour of the client.
     *
     * @type string        $body_encoding  Encoding to use for the event body, 'json', and 'msgpack' are possible values.
     * @type string        $event_encoding Encoding for the event itself to use 'json', and 'protobuf' are valid values.
     * }
     *
     * @throws ClientException if $body_encoding or $event_encoding are not supported values
     */
    public function __construct(GuzzleClient $client, $opts = []) {
        $this->client = $client;
        $this->getHostFromClient($client);

        $this->setBodyEncoding($opts);
        $this->setEventEncoding($opts);
    }

    /**
     * @param GuzzleClient $client
     *
     * @throws ClientException
     */
    private function getHostFromClient(GuzzleClient $client): void {
        $this->host = $client->getBaseUrl();
        if ($this->host === '') {
            throw new ClientException('Guzzle Client did not have a url');
        }
    }

    /**
     * @param $opts
     *
     * @throws ClientException
     */
    private function setBodyEncoding($opts): void {
        switch ($opts['body_encoding'] ?? null) {
            case null:
            case 'json':
                $this->body_encoding = 'json';
                break;
            case 'msgpack':
                $this->packer        = new Packer();
                $this->body_encoding = 'msgpack';
                break;
            default:
                throw new ClientException("The body_encoding '" . $opts['body_encoding'] . "' is not supported");
        }
    }

    /**
     * @param $opts
     *
     * @throws ClientException
     */
    private function setEventEncoding($opts): void {
        switch ($opts['event_encoding'] ?? null) {
            case null:
            case 'json':
                $this->event_encoding = 'json';
                break;
            case 'protobuf':
                $this->event_encoding = 'protobuf';
                break;
            default:
                throw new ClientException("The event_encoding '" . $opts['event_encoding'] . "' is not supported");
        }
    }

    /**
     * Sends an event to the Tremor host.
     *
     * @param array|string $body        Data to be send to tremor, if the 'body-encoding' raw is used the body will be send as is without additional encoding.
     * @param array        $opts        {
     *                                  Optional. Options to be passed to control the behaviour of the.
     *                                   @type Uuid $parent_uuid If the event has a parent event, the parent UUID can be specified here.
     *                                  }
     *
     * @return StreamInterface Returns the phrase passed in
     * @throws TremorException if there was an error when submitting the event.
     */
    public function publish($body, $opts = []): StreamInterface {
        $header = $this->createHeader($opts);

        try {
            return $this->publishBodyWithHeaderToTremor($body, $header);
        } catch (GuzzleClientException $e) {
            $this->throwExceptionFromGuzzleClientException($e);
        }

        throw new TremorException('could not publish the message');
    }

    /**
     * @param array $opts
     *
     * @return Proto\Event\Header
     * @throws TremorException
     */
    private function createHeader(array $opts): Proto\Event\Header {
        try {
            $headerData = [
                'uuid'         => Uuid::uuid1()->getBytes(),
                'content_type' => Proto\Event\Header\ContentType::JSON,
            ];
            $header     = new Proto\Event\Header($headerData);

            if ($opts['parent_uuid'] ?? false) {
                $header->setParentUuid($opts['parent_uuid']->getBytes());
            }
        } catch (\Exception $exception) {
            throw new TremorException("Header generation failed with the error '" . $exception->getMessage() . "'.");
        }

        return $header;
    }

    /**
     * @param                    $body
     * @param Proto\Event\Header $header
     *
     * @return StreamInterface|null
     * @throws TremorException
     */
    private function publishBodyWithHeaderToTremor($body, Proto\Event\Header $header): StreamInterface {
        $uri      = $this->host . '/v1';
        $options  = $this->createOptionsForBodyAndHeader($body, $header);
        $response = $this->client->put($uri, $options);
        if ($response === null) {
            throw new TremorException('did not get a response from request');
        }

        $responseBody = $response->getBody();
        if ($responseBody === null) {
            throw new TremorException('response did not have a body');
        }

        return $responseBody;
    }

    /**
     * @param                    $body
     * @param Proto\Event\Header $header
     *
     * @return array
     * @throws TremorException
     */
    private function createOptionsForBodyAndHeader($body, Proto\Event\Header $header): array {
        try {
            $e           = $this->createEventForBodyAndHeader($body, $header);
            $renderedMsg = $this->getRenderedMessageFromEvent($e);
            $contentType = $this->getContentTypeForRequest();

            return ['headers' => ['Content-Type' => $contentType], 'body' => $renderedMsg];
        } catch (ClientException $e) {
            throw new TremorException("Event submission failed with the error '" . $e->getMessage() . "'.");
        }
    }

    /**
     * @param                    $body
     * @param Proto\Event\Header $header
     *
     * @return Proto\Event
     * @throws ClientException
     */
    private function createEventForBodyAndHeader($body, Proto\Event\Header $header): Proto\Event {
        return new Proto\Event(['header' => $header, 'body' => $this->getEncodedBody($body)]);
    }

    /**
     * @param mixed $body the body to encode
     *
     * @return string
     * @throws ClientException
     */
    private function getEncodedBody($body): string {
        switch ($this->body_encoding) {
            case 'json':
                return json_encode($body);
            case 'raw':
                return $body;
            case 'msgpack':
                return $this->packer->pack($body);
            default:
                throw new ClientException('The body_encoding is not supported');
        }
    }

    /**
     * @param Proto\Event $e
     *
     * @return string
     * @throws ClientException
     */
    private function getRenderedMessageFromEvent(Proto\Event $e): string {
        switch ($this->event_encoding) {
            case 'json':
                return $e->serializeToJsonString();
                break;

            case 'protobuf':
                return $e->serializeToString();
                break;

            default:
                throw new ClientException('The event_encoding is not supported');
        }
    }

    /**
     * @return string
     * @throws ClientException
     */
    private function getContentTypeForRequest(): string {
        switch ($this->event_encoding) {
            case 'json':
                return 'application/json+tremor.v1';
                break;
            case 'protobuf':
                return 'application/protobuf+tremor.v1';
                break;
        }

        throw new ClientException('The event_encoding is not supported');
    }

    /**
     * @param GuzzleClientException $e
     *
     * @throws TremorException
     */
    private function throwExceptionFromGuzzleClientException(GuzzleClientException $e): void {
        $requestResponse = $e->getResponse();
        if ($requestResponse === null) {
            $exception = new TremorException('Client exception did not have a response embedded');
            $exception->setHTTPError($e);

            throw $exception;
        }

        $exception =
            new TremorException("Event submission failed with status code '" . $requestResponse->getStatusCode() . "'.");
        $exception->setHTTPError($e);

        throw $exception;
    }
}
