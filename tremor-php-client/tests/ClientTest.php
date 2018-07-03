<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Wayfair\Tremor\Client;
use Wayfair\Tremor\Exceptions;

final class BodyEncodingTest extends TestCase 
{
    public function testCanBeCreatedWithAnURL(): void{
        $this->assertInstanceOf(
            Client::class,
            new Client('http://127.0.0.1')
        );
    }
    public function testCanBeCreatedWithAnURLAndOptions(): void{
        $this->assertInstanceOf(
            Client::class,
            new Client('http://127.0.0.1', [])
        );
    }
    public function testCanBeCreatedWithAnURLAndJSONBodyEncoding(): void{
        $this->assertInstanceOf(
            Client::class,
            new Client('http://127.0.0.1', ['body_encoding' => 'json'])
        );
    }
    public function testCanBeCreatedWithAnURLAndMsgPackBodyEncoding(): void{
        $this->assertInstanceOf(
            Client::class,
            new Client('http://127.0.0.1', ['body_encoding' => 'msgpack'])
        );
    }
    public function testFailsWithBadBodyEncoding(): void{
        $this->expectException(Exceptions\ClientException::class);
        new Client('http://127.0.0.1', ['body_encoding' => 'blargh']);

    }
    public function testCanBeCreatedWithAnURLAndJSONEventEncoding(): void{
        $this->assertInstanceOf(
            Client::class,
            new Client('http://127.0.0.1', ['event_encoding' => 'json'])
        );
    }
    public function testCanBeCreatedWithAnURLAndMsgPackEventEncoding(): void{
        $this->assertInstanceOf(
            Client::class,
            new Client('http://127.0.0.1', ['event_encoding' => 'protobuf'])
        );
    }
    public function testFailsWithBadEventEncoding(): void{
        $this->expectException(Exceptions\ClientException::class);
        new Client('http://127.0.0.1', ['event_encoding' => 'blargh']);

    }
}
?>
