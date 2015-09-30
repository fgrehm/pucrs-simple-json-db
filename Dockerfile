FROM gliderlabs/alpine:3.2
MAINTAINER Fabio Rehm "fgrehm@gmail.com"

RUN apk-install bash \
                build-base \
                bzr \
                curl \
                git \
                mercurial \
                python \
                bash-completion

ENV HOME="/home/developer" \
    GOROOT="/usr/lib/go" \
    GOPATH="/go" \
    GOBIN="/go/bin" \
    PATH="/home/developer/bin:/go/bin:/usr/lib/go/bin:$PATH"

RUN set -x \
    && mkdir -p $HOME/bin \
    && echo 'source /etc/profile.d/bash_completion.sh' >> $HOME/.bashrc \
    && echo "alias ll='ls -lah'" >> $HOME/.bashrc \
    && curl -Ls https://github.com/progrium/basht/releases/download/v0.1.0/basht_0.1.0_Linux_x86_64.tgz \
       | tar -zxC $HOME/bin \
    && chmod +x $HOME/bin/basht \
    && curl -L https://storage.googleapis.com/golang/go1.5.linux-amd64.tar.gz \
       | tar xz -C $(dirname $GOROOT) \
    && ln -s lib lib64 \
    && go get github.com/parkghost/watchf/... \
    && go get github.com/constabulary/gb/... \
    && rm -rf /tmp/*

RUN set -x \
    && addgroup developer -g 1000 \
    && adduser -u 1000 -D -s /bin/bash -G developer developer \
    && chown 1000:1000 -R $HOME \
    && mkdir -p $GOPATH \
    && chown 1000:1000 -R $GOPATH \
    && mkdir -p $GOROOT \
    && chown 1000:1000 -R $GOROOT

USER developer
WORKDIR /code
CMD /bin/bash
