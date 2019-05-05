'''
    定时任务，每天自动更新通达性配置文件，
    保证行业配置信息和st股票及时更新 
'''
from git import Repo
import os 
import shutil 
import datetime 

dt = datetime.datetime.today()
sourcefolder = "D://new_tdx/"

rw_dir = "E://Schedule Tasks/"
targetfolder = os.path.join(rw_dir, 'repo/src/files/')
if not os.path.exists(os.path.join(rw_dir, 'repo')):
    repo = Repo.clone_from("git@github.com:xuhshen/stock_mock.git", os.path.join(rw_dir, 'repo'), branch='master')
else:
    repo = Repo( os.path.join(rw_dir, 'repo'))
    repo.head.reset(index=True, working_tree=True)
    repo.remotes.origin.pull(rebase=True)

files = ["incon.dat","T0002/hq_cache/tdxhy.cfg","T0002/hq_cache/tdxzs.cfg"]
 
for f in files:
    shutil.copy(os.path.join(sourcefolder, f),os.path.join(targetfolder, f.split("/")[-1]))
 
if repo.index.diff(None):
    changefiles = [f.a_path for f in repo.index.diff(None)]
    repo.index.add(changefiles)
    repo.index.commit("update tdx config files on date:{}".format(dt))
    
repo.remotes.origin.push()
     

