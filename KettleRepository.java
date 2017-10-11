package com.ksg.dcs;


import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component  
public class KettleRepository {

	@Value("${kettle.datasource.ip}")
	private String ip;
	@Value("${kettle.datasource.name}")
	private String dataBase;
	@Value("${kettle.datasource.username}")
	private String userName;
	@Value("${kettle.datasource.password}")
	private String password;
	@Value("${kettle.resources.name}")
	private String resourcesName;
	@Value("${kettle.resources.password}")
	private String resourcesPassword;

	private String post = "3306";

	private String connectionName = "java_kettle";
	private String connectionType = "mysql";
	private String connectionMode = "jdbc";

	/**
	 * 配置资源库环境 并接连接的资源库
	 * 
	 * @return
	 */
	public KettleDatabaseRepository repositoryCon() {
		// 数据库形式的资源库对象
		KettleDatabaseRepository rep = new KettleDatabaseRepository();
		try {
			System.err.println(ip);
			 //初始化
			KettleEnvironment.init();
			DatabaseMeta dataMeta = new DatabaseMeta(connectionName, connectionType, connectionMode, ip, dataBase, post,
					userName, password);
			/*DatabaseMeta dataMeta = new DatabaseMeta("128", "mysql", "jdbc",
					 "192.168.105.121", "db_ksg_kettle", "3306",
					 "mgcuser", "3g2win");*///连接名称 ，
			// 数据库形式的资源库元对象
			KettleDatabaseRepositoryMeta repInfo = new KettleDatabaseRepositoryMeta();
			//
			repInfo.setConnection(dataMeta);

			// 用资源库元对象初始化资源库对象
			rep.init(repInfo);
			// 连接到资源库
			rep.connect(resourcesName, resourcesPassword);// 默认的连接资源库的用户名和密码
			if (rep.isConnected()) {
				System.out.println("连接成功");
				return rep;
			} else {
				System.out.println("连接失败");
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rep;
	}

	/**
	 * 
	 * @param transName
	 * 要调用的trans名称
	 * 调用资源库中的trans
	 */
	public void runTrans(KettleDatabaseRepository repository,String transName) {

		try {
			RepositoryDirectoryInterface dir = repository.findDirectory("/");// 根据指定的字符串路径
			// 找到目录
			TransMeta tmeta = repository.loadTransformation(repository.getTransformationID(transName, dir), null);
			// 设置参数
			// tmeta.setParameterValue("", "");
			Trans trans = new Trans(tmeta);
			trans.execute(null);// 执行trans
			trans.waitUntilFinished();
			if (trans.getErrors() > 0) {
				System.out.println("有异常");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据指定的资源库对象及job名称 运行指定的job
	 * 
	 * @param rep
	 * @param jobName
	 */
	public Result runJob(KettleDatabaseRepository repository, String jobName) {
		try {
			RepositoryDirectoryInterface dir = repository.findDirectory("/");// 根据指定的字符串路径
			// 加载指定的job
			JobMeta jobMeta = repository.loadJob(repository.getJobId(jobName, dir), null);
			Job job = new Job(repository, jobMeta);
			// 设置参数
			// jobMeta.setParameterValue("method", "update");
			// jobMeta.setParameterValue("tsm5",
			// "07bb40f7200448b3a544786dc5e28845");
			// jobMeta.setParameterValue("args", "
			// {'fkid':'07bb40f7200448b3a544786dc5e28845','svctype':'Diffwkrlifehelp','content':'更新3','sysuuid':'01ee0e61f357476b8dbb4be49ddecc77','uid':'1033','role':'3999','posi':'2999'}");
			job.setLogLevel(LogLevel.ERROR);
			// 启动执行指定的job
			job.run();
			job.waitUntilFinished();// 等待job执行完；
			job.setFinished(true);
			System.err.println(job.getResult());
			return job.getResult();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	
}
