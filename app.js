const cron = require('node-cron');

const mysql = require('mysql');  // mysql 모듈 로드
const connection = mysql.createConnection({  // mysql 접속 설정
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
  
  // 연속 일수 작업 : 매일 자정 실행
  cron.schedule('0 0 * * *', () => {

   //업데이트 쿼리
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
  
    connection.query(updateQuery, (err, results) => {
      if (err) {
        console.error('Error executing the update query:', err);
        return;
      }
      console.log('Update query executed successfully', results);
    });
  }, {
    scheduled: true,
    timezone: "Asia/Seoul" // 원하는 시간대로 설정하세요
  });
  
  // 회원 동기화 작업 : 매일 4시 실행
  cron.schedule('0 4 * * *', () => {
    
   //activity_num 업데이트 쿼리
  const updateQuery = `
        UPDATE user a
        LEFT JOIN (
            SELECT a.uid, 
                  COUNT(DISTINCT b.id) AS activity_count
            FROM user a
            LEFT JOIN activity b
            ON a.uid = b.uid
              AND a.del_yn = 'N'
              AND b.del_yn = 'N'
            GROUP BY a.uid
        ) AS activity_counts ON a.uid = activity_counts.uid
        SET a.activity_num = COALESCE(activity_counts.activity_count, 0);
    `;
  
    connection.query(updateQuery, (err, results) => {
      if (err) {
        console.error('Error executing the update query:', err);
        return;
      }
      console.log('Update query executed successfully', results);
    });
  }, {
    scheduled: true,
    timezone: "Asia/Seoul" // 원하는 시간대로 설정하세요
  });

  // following_num 작업 : 매일 자정 실행
  cron.schedule('15 4 * * *', () => {

  //following_num 업데이트 쿼리
  const updateQuery2 = `
      UPDATE user u
      LEFT JOIN (
          SELECT uid, 
                COUNT(DISTINCT following_uid) AS following_count
          FROM follow f
          WHERE del_yn = 'N'
          GROUP BY uid
      ) AS follow_counts ON u.uid = follow_counts.uid
      SET u.following_num = COALESCE(follow_counts.following_count, 0);
    `;

    connection.query(updateQuery2, (err, results) => {
    if (err) {
      console.error('Error executing the update query:', err);
      return;
    }
    console.log('Update query executed successfully', results);
    });
  }, {
    scheduled: true,
    timezone: "Asia/Seoul" // 원하는 시간대로 설정하세요
  });


  // follower_num :  실행
  cron.schedule('30 4 * * *', () => {

  //follower_num 업데이트 쿼리
  const updateQuery3 = `
      UPDATE user u
      LEFT JOIN (
          SELECT following_uid, 
                COUNT(DISTINCT uid) AS follower_count
          FROM follow f
          WHERE f.del_yn = 'N'  
          GROUP BY following_uid
      ) AS follower_counts ON u.uid = follower_counts.following_uid
      SET u.follower_num = COALESCE(follower_counts.follower_count, 0);
    `;

    connection.query(updateQuery3, (err, results) => {
    if (err) {
      console.error('Error executing the update query:', err);
      return;
    }
    console.log('Update query executed successfully', results);
    });
  }, {
    scheduled: true,
    timezone: "Asia/Seoul" // 원하는 시간대로 설정하세요
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