FROM python:3.7
ENV PATH /opt/conda/bin:$PATH

RUN mkdir /app && cd /app

RUN apt-get -y update && \
    apt-get clean && \
    wget --progress=bar:force https://repo.anaconda.com/archive/Anaconda3-2022.10-Linux-x86_64.sh -O /opt/anaconda.sh && \
    /bin/bash /opt/anaconda.sh -b -p /opt/conda && \
    rm /opt/anaconda.sh && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc && \
    conda install -c bih-cubi bcl2fastq2 && \
    conda install -c conda-forge s5cmd && \
    export PROFILE=svc-ihg

RUN pip install interop && \
    pip install awscli

COPY bcl2_fastq.py run_qc_yaml_interop.py  /

ENTRYPOINT ["python", "/bcl2_fastq.py"]
