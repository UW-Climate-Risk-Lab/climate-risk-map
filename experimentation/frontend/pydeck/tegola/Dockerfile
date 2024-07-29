FROM gospatial/tegola:v0.16.0

ENV BOUNDS="-180,-85.0511,180,85.0511" \
    DB_URI=postgres://osm_ro_user:mysecretpassword@host.docker.internal:5432/pgosm_flex_washington
    
COPY config.toml /etc/tegola/config.toml

EXPOSE 8070
ENTRYPOINT ["/opt/tegola"]
CMD ["serve", "--config", "/etc/tegola/config.toml"]