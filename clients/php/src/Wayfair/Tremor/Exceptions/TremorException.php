<?php

namespace Wayfair\Tremor\Exceptions;

use GuzzleHttp\Exception\ClientException as GuzzleClientException;

/**
 * Class TremorException
 *
 * Exception thrown for errors during event submission.
 *
 * @package Wayfair\Tremor\Exceptions
 */
class TremorException extends \Exception
{
    public const DEFAULT_ERROR_STATUS_CODE = 502;

    /**
     * @var GuzzleClientException
     */
    private $http_error;

    public function setHTTPError(GuzzleClientException $error): void
    {
        $this->http_error = $error;
    }

    public function getStatusCode(): int
    {
        if ($this->http_error !== null) {
            $response = $this->http_error->getResponse();
            if ($response !== null) {
                return $response->getStatusCode();
            }
        }

        return self::DEFAULT_ERROR_STATUS_CODE;
    }
}
