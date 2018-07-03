# Getting Started with Tremor

A guide to help you get Tremor up and running on your machine.

## Prerequisites

You should have Docker and Git installed and configured. You will need to have access to the engineering vlan VPN.

## Repository Set-Up

You will need to open a terminal, and clone the repo. This repo has the Dockerfile, which builds the image for Tremor.

To do this, type in the shell:

```sh
git clone github.com/Wayfair/tremor
```

## Tremor Docker Image

You will need to be in the tremor-runtime directory to make a Docker image.

To change to tremor-runtime directory, type in the shell:

```sh
cd tremor-runtime
```

Now that you are in the tremor-runtime directory enter:

```sh
make tremor-image
```

This is creating the local Docker image `tremor-runtime`.

## Container Configuration

Is done passing in a config.yaml file.

After completing the `exmaple.yaml`, you need to start the container, type in the shell:

```sh
docker-compose -f path/to/your/example.yaml up
```
