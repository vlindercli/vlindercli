# Docker/podman to up all the necessary prerequisites

## Topology

The deployment consists of the following components

* Ollama
* Nats server with jet stream enabled

## Demonstration

```
podman compose up --build
```
Above command will download rust image, will build current vlindercli repo code and create image. Then it will pull Nats image and ollama image. Once nats, ollama containers are up, vlinderd container will get up. Please download your preferred ollama model. Once all 3 containers are up, you can use vlinder tool to create and deploy agents.


## Near Future optimisation

Right now we're not using already downloaded dependencies while building vlinderd image. Everytime we're building vlinderd image, it will download all the dependencies. I will check how to improve that. Also we're downloading rust image which is around 1 GB in size. But we will very shortly move to trixie-slim image which is expected to be much smaller.
