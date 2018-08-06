FROM alpine
RUN apk update && apk upgrade && apk add vim ca-certificates

