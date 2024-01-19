FROM rust

# Install Google Cloud SDK
RUN apt-get update -y && apt-get install -y curl python3-dev python3-setuptools apt-transport-https lsb-release gnupg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN apt-get update -y && apt-get install -y google-cloud-sdk

COPY . .

EXPOSE 8080:8080

RUN cargo build --release

CMD [ "./target/release/src_rust" ]