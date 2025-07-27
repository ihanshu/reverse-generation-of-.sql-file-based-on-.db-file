public class Main {

	DbUser myDbUser = null;

	private void go() {
		String databaseName = "Chinook" ; //Modify here to select database backup
		System.out.println("In go...");
		myDbUser = new DbUser(databaseName+".db");

		String outputFilePath = "database_backup_"+ databaseName+ ".sql";
		myDbUser.generateSqlBackup(outputFilePath);

		myDbUser.close();
	}

	public static void main(String[] args) {
		Main myMain = new Main();
		myMain.go();
	}

}