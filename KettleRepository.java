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
	 * ������Դ�⻷�� �������ӵ���Դ��
	 * 
	 * @return
	 */
	public KettleDatabaseRepository repositoryCon() {
		// ���ݿ���ʽ����Դ�����
		KettleDatabaseRepository rep = new KettleDatabaseRepository();
		try {
			System.err.println(ip);
			 //��ʼ��
			KettleEnvironment.init();
			DatabaseMeta dataMeta = new DatabaseMeta(connectionName, connectionType, connectionMode, ip, dataBase, post,
					userName, password);
			/*DatabaseMeta dataMeta = new DatabaseMeta("128", "mysql", "jdbc",
					 "192.168.105.121", "db_ksg_kettle", "3306",
					 "mgcuser", "3g2win");*///�������� ��
			// ���ݿ���ʽ����Դ��Ԫ����
			KettleDatabaseRepositoryMeta repInfo = new KettleDatabaseRepositoryMeta();
			//
			repInfo.setConnection(dataMeta);

			// ����Դ��Ԫ�����ʼ����Դ�����
			rep.init(repInfo);
			// ���ӵ���Դ��
			rep.connect(resourcesName, resourcesPassword);// Ĭ�ϵ�������Դ����û���������
			if (rep.isConnected()) {
				System.out.println("���ӳɹ�");
				return rep;
			} else {
				System.out.println("����ʧ��");
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
	 * Ҫ���õ�trans����
	 * ������Դ���е�trans
	 */
	public void runTrans(KettleDatabaseRepository repository,String transName) {

		try {
			RepositoryDirectoryInterface dir = repository.findDirectory("/");// ����ָ�����ַ���·��
			// �ҵ�Ŀ¼
			TransMeta tmeta = repository.loadTransformation(repository.getTransformationID(transName, dir), null);
			// ���ò���
			// tmeta.setParameterValue("", "");
			Trans trans = new Trans(tmeta);
			trans.execute(null);// ִ��trans
			trans.waitUntilFinished();
			if (trans.getErrors() > 0) {
				System.out.println("���쳣");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ����ָ������Դ�����job���� ����ָ����job
	 * 
	 * @param rep
	 * @param jobName
	 */
	public Result runJob(KettleDatabaseRepository repository, String jobName) {
		try {
			RepositoryDirectoryInterface dir = repository.findDirectory("/");// ����ָ�����ַ���·��
			// ����ָ����job
			JobMeta jobMeta = repository.loadJob(repository.getJobId(jobName, dir), null);
			Job job = new Job(repository, jobMeta);
			// ���ò���
			// jobMeta.setParameterValue("method", "update");
			// jobMeta.setParameterValue("tsm5",
			// "07bb40f7200448b3a544786dc5e28845");
			// jobMeta.setParameterValue("args", "
			// {'fkid':'07bb40f7200448b3a544786dc5e28845','svctype':'Diffwkrlifehelp','content':'����3','sysuuid':'01ee0e61f357476b8dbb4be49ddecc77','uid':'1033','role':'3999','posi':'2999'}");
			job.setLogLevel(LogLevel.ERROR);
			// ����ִ��ָ����job
			job.run();
			job.waitUntilFinished();// �ȴ�jobִ���ꣻ
			job.setFinished(true);
			System.err.println(job.getResult());
			return job.getResult();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	
}
