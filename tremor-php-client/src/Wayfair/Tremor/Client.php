<?php
/**
 * Tremor client for PHP
 */

declare(strict_types=1);

namespace Wayfair\Tremor;

require_once "vendor/autoload.php";

use GuzzleHttp;
use Ramsey\Uuid\Uuid;
use MessagePack\Packer;
use Wayfair\Tremor\Proto;
use Wayfair\Tremor\Exceptions\ClientException;
use Wayfair\Tremor\Exceptions\TremorException;

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
     * Hostname of the Trmor instnace
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
     */
    private $packer;

    /**
     * Create a new Tremor Client Instance
     * @param string $host URL of the tremor host to send events to.
     * @param array $opts {
     *     Optional. Options to be passed to control the behaviour of the client.
     *     @type string $body_encoding Encoding to use for the event body, 'json', and 'msgpack' are possible values.
     *     @type string $event_encoding Encoding for the event itself to use 'json', and 'protobuf' are valid values.
     * }
     *
     * @throws ClientException if $body_encoding or $event_encoding are not supported values
     */
    public function __construct($host, $opts = [])
    {
        $this->client = new GuzzleHttp\Client(['base_uri' => $host]);
        $this->host = $host;
        switch ($opts['body_encoding']) {
        case null:
        case 'json':
            $this->body_encoding = 'json';
            break;
        case 'msgpack':
            $this->packer = new Packer();
            $this->body_encoding = 'msgpack';
            break;
        default:
            throw new ClientException("The body_encoding '". $opts['body_encoding'] . "' is not supported");

        };
        switch ($opts['event_encoding']) {
        case null:
        case 'json':
            $this->event_encoding = 'json';
            break;
        case 'protobuf':
            $this->event_encoding = 'protobuf';
            break;
        default:
            throw new ClientException("The event_encoding '". $opts['event_encoding'] . "' is not supported");
        };

    }

    /**
     * Sends an event to the Tremor host.
     *
     * @param array|string $body Data to be send to tremor, if the 'body-encoding' raw is used the body will be send as is without additional encoding.
     * @param array $opts {
     *     Optional. Options to be passed to control the behaviour of the.
     *
     *     @type Ramsey\Uuid\Uuid $parent_uuid If the event has a parent event, the parent UUID can be specified here.
     * }
     *
     * @return string Returns the phrase passed in
     * @throws TremorException if there was an error when submitting the event.
     */
    public function publish($body, $opts = [])
    {
        $h = new Proto\Event\Header([
            'uuid' => Uuid::uuid1()->getBytes(),
            'content_type' => Proto\Event\Header\ContentType::JSON
        ]);
        //        $h->setContentType(Proto\Event\Header\ContentType::MSGPACK);
        if ($opts['parent_uuid'] != null) {
            $h->setParentUuid($opts['parent_uuid']->getBytes());
        };
        //        $h->setContentType();
        switch ($this->body_encoding) {
        case 'json':
            $data = json_encode($body);
            break;
        case 'raw':
            $data = $body;
            break;
        case 'msgpack':
            $data = $this->packer->pack($body);
            break;
        };
        $e = new Proto\Event(['header' => $h, 'body' => $data]);
        switch ($this->event_encoding) {
        case 'json':
            $content_type = 'application/json+tremor.v1';
            $rendered_msg = $e->serializeToJsonString();
            break;
        case 'protobuf':
            $content_type = 'application/protobuf+tremor.v1';
            $rendered_msg = $e->serializeToString();
            break;
        };
        try {
            $response = $this->client->request('PUT', '/v1', ['ContentType' => $content_type, 'body' => $rendered_msg]);
            return $response->getBody();
        } catch (GuzzleHttp\Exception\ClientException $e) {
            $exception = new TremorException("Event submission failed with status code '". $e->getResponse()->getStatusCode() . "'.");
            $exception->setHTTPError($e);
            throw $exception;
        }
    }
}
