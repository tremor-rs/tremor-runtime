<?php
namespace Wayfair\Tremor\Exceptions;

/**
 * Class TremorException
 *
 * Exception thrown for errors during event submission.
 *
 * @package Wayfair\Tremor\Exceptions
 */
class TremorException extends \Exception {
    private $http_error;

    public function setHTTPError($error) {
        $this->http_error = $error;
    }

    public function getStatusCode() {
        if ($this->http_error != null) {
            return $this->http_error->getResponse()->getStatusCode();
        }
    }

}
?>
