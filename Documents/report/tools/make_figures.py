from pathlib import Path
import json
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.colors import ListedColormap
root=Path(__file__).resolve().parent.parent
data=json.loads((root/'evidence/verification_summary.json').read_text())
out=root/'figures'
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':9.2,'axes.titlesize':10,'axes.labelsize':9.2,
                     'xtick.labelsize':8.8,'ytick.labelsize':8.8,'pdf.fonttype':42,'ps.fonttype':42,
                     'axes.spines.top':False,'axes.spines.right':False,'axes.edgecolor':'#667085',
                     'axes.labelcolor':'#273142','xtick.color':'#273142','ytick.color':'#273142'})
blue='#2568a5';orange='#d48b29';teal='#21887a';grey='#a5b3c3';ink='#273142'
def vi(x,dec=0):return f'{x:,.{dec}f}'.replace(',','@').replace('.',',').replace('@','.')
def save(fig,name):
    fig.savefig(out/f'{name}.pdf',bbox_inches='tight',pad_inches=.08)
    
    plt.close(fig)
fig,ax=plt.subplots(figsize=(6.25,2.7),layout='constrained')
names=['Source (summary)','Bridge (summary)','Bronze (đếm lại)','Silver (đếm lại)','PG theo run (health)']
ax.barh(np.arange(5),[50.760004]*5,color=[grey,grey,blue,blue,grey],height=.58)
for i in range(5):ax.text(50.760004+.65,i,'50.760.004',va='center',fontsize=8.7)
ax.set(yticks=np.arange(5),yticklabels=names,xlim=(0,63),xlabel='Triệu sự kiện / dòng trong cùng run')
ax.invert_yaxis();ax.xaxis.grid(True,color='#e3e8ee');ax.set_axisbelow(True);save(fig,'ch4-payload-accounting')
fig,ax=plt.subplots(figsize=(6.25,2.5),layout='constrained')
matrix=np.array([[1,1,0,1,0,1],[0,0,1,0,1,0],[1,1,1,1,1,1]])
ax.imshow(matrix,cmap=ListedColormap(['#ecf0f4','#2868a4']),vmin=0,vmax=1,aspect='auto')
ax.set(xticks=range(6),xticklabels=['Nhiệt độ','Độ ẩm',r'CO$_2$','Áp suất','PM2.5','Ánh sáng'],
       yticks=range(3),yticklabels=['Thời tiết','Không khí','Đa chỉ số'])
for y in range(3):
 for x in range(6):ax.text(x,y,'100%' if matrix[y,x] else '0%',ha='center',va='center',color='white' if matrix[y,x] else ink)
ax.tick_params(length=0)
for sp in ax.spines.values():sp.set_visible(False)
save(fig,'ch4-metric-presence')
hours=data['hourly_event_stats']
events=np.array([h['events'] for h in hours])
alerts=np.array([sum(h[k] for k in ['temperature_alerts','humidity_alerts','co2_alerts','pressure_alerts','pm25_alerts']) for h in hours])
rates=alerts/events*100
fig,(a,b)=plt.subplots(2,1,figsize=(6.25,4.0),sharex=True,layout='constrained',gridspec_kw={'height_ratios':[2,1]})
a.bar(range(5),events/1e6,color=[grey,blue,blue,blue,grey],width=.6)
for x,n in enumerate(events):a.text(x,n/1e6+.2,vi(n/1e6,3),ha='center',fontsize=8.8)
a.set(ylabel='Triệu sự kiện',ylim=(0,14.8))
a.text(0,1.1,'~17 phút',ha='center',fontsize=8.2,color=ink)
a.text(4,1.1,'~43 phút',ha='center',fontsize=8.2,color=ink)
b.plot(range(5),rates,color=teal,marker='o',markersize=4)
for x,y in enumerate(rates):b.text(x,y+.13,vi(y,3),ha='center',fontsize=8.5)
b.set(ylabel='Lượt cảnh báo\n/ 100 sự kiện',ylim=(0,2.2),xticks=range(5),
      xticklabels=['03:00','04:00','05:00','06:00','07:00'],xlabel='Giờ Việt Nam (UTC+7), ngày 02/10/2026')
for ax in [a,b]:ax.yaxis.grid(True,color='#e3e8ee');ax.set_axisbelow(True)
save(fig,'ch4-hourly')
stats=data['metric_stats'][:5]
fig,ax=plt.subplots(figsize=(6.25,2.8),layout='constrained')
labels=['Nhiệt độ','Độ ẩm',r'CO$_2$','Áp suất','PM2.5']
values=[m['alert_pct_of_present_readings'] for m in stats]
bars=ax.barh(range(5),values,color=[blue,blue,orange,blue,orange],height=.58)
for y,v in enumerate(values):ax.text(v+.015,y,vi(v,4)+'%',va='center',fontsize=8.8)
ax.set(yticks=range(5),yticklabels=labels,xlim=(0,1),xlabel='Lượt cảnh báo / reading hiện diện (%)')
ax.xaxis.set_major_formatter(FuncFormatter(lambda x,p:vi(x,1)))
ax.invert_yaxis();ax.xaxis.grid(True,color='#e3e8ee');ax.set_axisbelow(True);save(fig,'ch4-alert-rates')
qs=data['query_progress'];x=np.arange(4);w=.23
fig,ax=plt.subplots(figsize=(6.25,3.4),layout='constrained')
for j,(key,lab,c) in enumerate([('p50_ms','P50',blue),('p95_ms','P95',orange),('max_ms','Lớn nhất',teal)]):
 vals=np.array([q[key] for q in qs])
 ax.bar(x+(j-1)*w,vals/1000,width=w*.9,color=c,label=lab)
 for xx,v in zip(x+(j-1)*w,vals):
  ax.text(xx,v/1000+.085,vi(v/1000,3),ha='center',va='bottom',fontsize=7.4,rotation=90)
ax.axhline(5,ls='--',lw=1.2,color='#667085',label='Trigger 5 s')
ax.set(xticks=x,xticklabels=['Q1\nBronze','Q2a\nSilver','Q2b\nQuarantine','Q3\nPostgreSQL'],
       ylabel='Thời gian xử lý micro-batch (s)',ylim=(0,5.85))
ax.legend(loc='upper left',frameon=False,ncol=4,fontsize=8.2)
ax.yaxis.grid(True,color='#e3e8ee');ax.set_axisbelow(True);save(fig,'ch4-microbatch')
wanted=['bigdata-spark-stream','bigdata-postgres','bigdata-kafka','bigdata-bridge','bigdata-backend','bigdata-dashboard','bigdata-mqtt']
resource={r['container']:r for r in data['resource_stats']}
labels=['Spark','PostgreSQL','Kafka','Bridge','Backend','Dashboard','Mosquitto']
caps=[2560,512,1024,128,256,64,64]
values=[resource[k]['peak_memory_pct'] for k in wanted]
fig,ax=plt.subplots(figsize=(6.25,3.4),layout='constrained')
ax.barh(range(7),values,color=[blue,orange,blue,grey,grey,grey,grey],height=.58)
for y,(k,cap,v) in enumerate(zip(wanted,caps,values)):
 ax.text(v+1.1,y,vi(v,2)+'%',va='center',fontsize=8.5)
ax.set(yticks=range(7),yticklabels=labels,xlim=(0,107),xlabel='Đỉnh bộ nhớ / giới hạn container (%)')
ax.invert_yaxis();ax.set_xticks([0,25,50,75,100]);ax.xaxis.grid(True,color='#e3e8ee');ax.set_axisbelow(True)
save(fig,'ch4-memory-peaks')
print('Created 6 vector PDF figures')
