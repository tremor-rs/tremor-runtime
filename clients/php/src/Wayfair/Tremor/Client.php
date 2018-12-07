<?php
declare(strict_types=1);

namespace Wayfair\Tremor;

use GuzzleHttp;
use MessagePack\Packer;
use Psr\Http\Message\StreamInterface;
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
class Client
{
    /**
     * HTTP client isntance to talk to Tremor
     */
    private $client;

    /**
     * Hostname of the Tremor instnace
     * @var string
     */
    private $host;

    /**
     * Body encoding to be used
     */
    private $body_encoding = 'json';

    /**
     * Event, or outer wrapper encoding to be used
     */
    private $event_encoding = 'json';

    /**
     * Packer instance for msgpack encoding
     * @var Packer
     */
    private $packer;

    /**
     * Create a new Tremor Client Instance
     * @param GuzzleHttp\Client $client client setup witht the URL of the tremor host to send events to.
     * @param array $opts {
     *      Optional. Options to be passed to control the behaviour of the client.
     * @type string $body_encoding Encoding to use for the event body, 'json', and 'msgpack' are possible values.
     * @type string $event_encoding Encoding for the event itself to use 'json', and 'protobuf' are valid values.
     * }
     *
     * @throws ClientException if $body_encoding or $event_encoding are not supported values
     */
    public function __construct(GuzzleHttp\Client $client, $opts = [])
    {
        $this->client = $client;

        $this->host = $client->getBaseUrl();
        if ($this->host === '') {
            throw new ClientException('Guzzle Client did not have a url');
        }

        $this->setBodyEncoding($opts);
        $this->setEventEncoding($opts);
    }

    /**
     * Sends an event to the Tremor host.
     *
     * @param array|string $body Data to be send to tremor, if the 'body-encoding' raw is used the body will be send as is without additional encoding.
     * @param array $opts {
     *      Optional. Options to be passed to control the behaviour of the.
     * @type Uuid $parent_uuid If the event has a parent event, the parent UUID can be specified here.
     * }
     *
     * @return StreamInterface Returns the phrase passed in
     * @throws TremorException if there was an error when submitting the event.
     */
    public function publish($body, $opts = []): StreamInterface
    {
        try {
            $header = new Proto\Event\Header([
                'uuid' => Uuid::uuid1()->getBytes(),
                'content_type' => Proto\Event\Header\ContentType::JSON
            ]);
        } catch (\Exception $exception) {
            throw new TremorException("Header generation failed with the error '" . $exception->getMessage() . "'.");
        }

        if ($opts['parent_uuid'] !== null) {
            $header->setParentUuid($opts['parent_uuid']->getBytes());
        }

        try {
            $e = new Proto\Event(['header' => $header, 'body' => $this->getEncodedBody($body)]);
            $renderedMsg = $this->getRenderedMessageFromEvent($e);
            $contentType = $this->getContentTypeForRequest();

            // Guzzle 6.0
            // $response = $this->client->request('PUT', '/v1', ['ContentType' => $content_type, 'body' => $rendered_msg]);
            $response = $this->client->put($this->host . '/v1',
                ['headers' => ['Content-Type' => $contentType], 'body' => $renderedMsg]);

            return $response->getBody();
        } catch (GuzzleHttp\Exception\ClientException $e) {
            $requestResponse = $e->getResponse();
            if ($requestResponse === null) {
                $exception = new TremorException('Client exception did not have a response embedded');
                $exception->setHTTPError($e);

                throw $exception;
            }

            $exception = new TremorException("Event submission failed with status code '" . $requestResponse->getStatusCode() . "'.");
            $exception->setHTTPError($e);

            throw $exception;
        } catch (ClientException $e) {
            throw new TremorException("Event submission failed with the error '" . $e->getMessage() . "'.");
        }
    }

    /**
     * @param $opts
     * @throws ClientException
     */
    protected function setEventEncoding($opts): void
    {
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
     * @param $opts
     * @throws ClientException
     */
    protected function setBodyEncoding($opts): void
    {
        switch ($opts['body_encoding'] ?? null) {
            case null:
            case 'json':
                $this->body_encoding = 'json';
                break;
            case 'msgpack':
                $this->packer = new Packer();
                $this->body_encoding = 'msgpack';
                break;
            default:
                throw new ClientException("The body_encoding '" . $opts['body_encoding'] . "' is not supported");
        }
    }

    /**
     * @param mixed $body the body to encode
     * @return string
     * @throws ClientException
     */
    protected function getEncodedBody($body): string
    {
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
    protected function getRenderedMessageFromEvent(Proto\Event $e): string
    {
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
    protected function getContentTypeForRequest(): string
    {
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
}
