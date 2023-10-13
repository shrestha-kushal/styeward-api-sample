FROM public.ecr.aws/ubuntu/ubuntu:22.10_stable
RUN apt update && apt install -y ca-certificates
COPY target/release/styeward-api /styeward-api

ENTRYPOINT ["/styeward-api"]
