FROM mcr.microsoft.com/devcontainers/python:3.11-bookworm

WORKDIR /app

# Evitar prompts interativos durante as instalações do apt
ENV DEBIAN_FRONTEND=noninteractive

# Instalar o OpenJDK 17 (Cria diretórios obrigatórios do Debian e corrige dependências)
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configurar as variáveis de ambiente para o Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# [Opcional] Atualizar o pip e instalar ferramentas comuns de desenvolvimento
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD python main.py