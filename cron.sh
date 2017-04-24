#!/usr/bin/env bash
curl https://cronitor.link/E1KGCc/run -m 10 || true
cd /root/theoutgroup/
python3.6 crawler.py
curl https://cronitor.link/E1KGCc/complete -m 10 || true