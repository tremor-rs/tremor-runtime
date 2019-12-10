# Setup local development environment for Mkdocs

This is a short canned synopsis of preparing a local development environment for Mkdocs development

## Install mkdocs

```bash
brew install mkdocs
```

## Install Material Theme

As mkdocs is Python-based but does not provide a plugin mechanism
we need to install the material theme for PyMarkdown directly via
pip3 or similar

```bash
pip3 install mkdocs-material
```

## Build documentation

```bash
mkdocs build
```

## Local doc service

```bash
mkdocs serve
```
