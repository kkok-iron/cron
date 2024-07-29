const cron = require('node-cron');

const mysql = require('mysql');  // mysql 모듈 로드
const conn = mysql.createConnection({  // mysql 접속 설정
    host: 'kkokiyo-mysql.c1kmsw8s42mh.ap-northeast-2.rds.amazonaws.com',
    port: '3306',
    user: 'root',
    password: 'kky240101!',
    database: 'kkokiyodb'
});  

// 데이터베이스 연결
connection.connect(err => {
    if (err) {
      console.error('Error connecting to the database:', err);
      return;
    }
    console.log('Connected to the database');
  });
  
  // 크론 작업 설정 매일 자정 실행
  cron.schedule('0 0 * * *', () => {
    console.log('Running the cron job every hour');
  
   // 업데이트 쿼리
  const updateQuery = `
        UPDATE user SET continue_num = 0
        WHERE uid IN (
            SELECT tmp.uid FROM (
            SELECT u.uid, COUNT(a.uid) AS cnt 
            FROM 
                user AS u
                LEFT OUTER JOIN activity AS a ON u.uid = a.uid AND DATE(a.reg_dt) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            WHERE 
                u.continue_num > 0
            GROUP BY u.uid
            ) AS tmp
            WHERE tmp.cnt = 0
        );
    `;
  
    connection.query(updateQuery, queryValues, (err, results) => {
      if (err) {
        console.error('Error executing the update query:', err);
        return;
      }
      console.log('Update query executed successfully', results);
    });
  }, {
    scheduled: true,
    timezone: "Asial/Seoul" // 원하는 시간대로 설정하세요
  });
  
  // 프로세스 종료 시 데이터베이스 연결 종료
  process.on('SIGINT', () => {
    connection.end(err => {
      if (err) {
        console.error('Error disconnecting from the database:', err);
      }
      console.log('Disconnected from the database');
      process.exit(0);
    });
  });