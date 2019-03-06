FROM 192.168.0.106:5000/basepython:latest

ENV workspace /home
WORKDIR ${workspace}
COPY src ./ 
COPY mock ./ 
COPY requirement.txt ./ 

RUN pip install  -r requirement.txt -i https://pypi.tuna.tsinghua.edu.cn/simple \
	&& rm -fr ~/.cache/pip \

ENV PYTHONPATH="${workspace}:$PYTHONPATH"

CMD [ "python","/home/simulation.py" ]