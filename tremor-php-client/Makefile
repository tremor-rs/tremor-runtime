run: src/Wayfair/Tremor/Proto/Event.php vendor/autoload.php
	docker run -d -p 8080:80 --name tremor-php-client -v "$(PWD)":/var/www/html php:7.2-apache

image:
	docker build . -t tremor-php-client

clean:
	rm -r vendor
	rm -r output

docs: vendor/bin/phpdoc
	./vendor/bin/phpdoc -d src/Wayfair/

test: vendor/bin/phpunit
	./vendor/bin/phpunit --bootstrap vendor/autoload.php tests/*

vendor:
	composer install

vendor/bin/phpdoc: vendor

vendor/bin/phpunit: vendor


src/Wayfair/Tremor/Proto/Event.php: ../tremor.proto
	protoc --php_out=src/ ../tremor.proto

vendor/autoload.php: composer.lock
	composer install
