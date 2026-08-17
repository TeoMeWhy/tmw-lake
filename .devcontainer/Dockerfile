FROM mcr.microsoft.com/devcontainers/python:3.11-bookworm

# Evitar prompts interativos durante as instalações do apt
ENV DEBIAN_FRONTEND=noninteractive

# Instalar o OpenJDK 17 (dependência necessária para o Apache Spark)
RUN apt-get update && apt-get -y install --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Configurar as variáveis de ambiente para o Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# [Opcional] Atualizar o pip e instalar ferramentas comuns de desenvolvimento
RUN pip install --no-cache-dir --upgrade pip
