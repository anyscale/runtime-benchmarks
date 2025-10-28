FROM anyscale/ray:2.50.1-slim-py312-cu128
RUN sudo apt-get update -y \
    && sudo apt-get install --no-install-recommends -y ffmpeg \
    && sudo rm -f /etc/apt/sources.list.d/*
RUN pip install --no-cache-dir --upgrade opencv-python torchvision Pillow yt-dlp

ENV RAY_experimental_object_manager_enable_multiple_connections=1
ENV RAY_object_manager_client_connection_num=15
