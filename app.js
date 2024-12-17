const cron = require('node-cron');
const mysql = require('mysql');
const axios = require('axios');

// MySQL 연결 설정
const dbConfig = {
    host: 'kkokiyo-mysql.c1kmsw8s42mh.ap-northeast-2.rds.amazonaws.com',
    port: '3306',
    user: 'root',
    password: 'kky240101!',
    database: 'kkokiyodb'
};

const connection = mysql.createConnection(dbConfig);

// 데이터베이스 연결 함수
function connectDB() {
    connection.connect(err => {
        if (err) console.error('Error connecting to the database:', err);
        else console.log('Connected to the database');
    });
}

// 쿼리 실행 함수
function executeQuery(query, params = [], successMessage = '') {
    return new Promise((resolve, reject) => {
        connection.query(query, params, (err, results) => {
            if (err) {
                console.error('Error executing query:', err);
                reject(err);
            } else {
                console.log(successMessage, results);
                resolve(results);
            }
        });
    });
}

// 1. 상품 리스트 저장 작업: 매일 새벽 2시
async function updateProductList() {
    const API_URL = 'https://bizapi.giftishow.com/bizApi/goods';
    const AUTH_CODE = 'REALb2f7bab33e0242928af994d65edbd461';
    const AUTH_TOKEN = 'Hl7xPtrvSb6XEdvdY7pOBA==';

    try {
        console.log('Starting product list update...');
        await executeQuery('TRUNCATE TABLE products', [], 'Products table truncated.');

        const response = await axios.post(API_URL, {
            api_code: '0101',
            custom_auth_code: AUTH_CODE,
            custom_auth_token: AUTH_TOKEN,
            dev_yn: 'N',
            start: '1',
            size: '100'
        });

        const products = response.data.result.goodsList;
        const insertQuery = `
            INSERT INTO products 
            (rmIdBuyCntFlagCd, discountRate, mdCode, endDate, affliateId, discountPrice, 
             mmsGoodsImg, srchKeyword, limitDay, content, goodsImgB, goodsTypeNm, exhGenderCd, 
             exhAgeCd, validPrdDay, goodsComName, goodsName, mmsReserveFlag, popular, goodsStateCd, 
             brandCode, goodsNo, brandName, mmsBarcdCreateYn, salePrice, brandIconImg, goodsComId, 
             rmCntFlag, saleDateFlagCd, contentAddDesc, goodsCode, goodsTypeDtlNm, category1Seq, 
             goodsImgS, affiliate, validPrdTypeCd, saleDateFlag, realPrice)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `;

        for (const product of products) {
            await executeQuery(insertQuery, [
                product.rmIdBuyCntFlagCd, product.discountRate, product.mdCode, product.endDate,
                product.affliateId, product.discountPrice, product.mmsGoodsImg, product.srchKeyword,
                product.limitDay, product.content, product.goodsImgB, product.goodsTypeNm,
                product.exhGenderCd, product.exhAgeCd, product.validPrdDay, product.goodsComName,
                product.goodsName, product.mmsReserveFlag, product.popular, product.goodsStateCd,
                product.brandCode, product.goodsNo, product.brandName, product.mmsBarcdCreateYn,
                product.salePrice, product.brandIconImg, product.goodsComId, product.rmCntFlag,
                product.saleDateFlagCd, product.contentAddDesc, product.goodsCode, product.goodsTypeDtlNm,
                product.category1Seq, product.goodsImgS, product.affiliate, product.validPrdTypeCd,
                product.saleDateFlag, product.realPrice
            ]);
        }
        console.log('Product list updated successfully.');
    } catch (error) {
        console.error('Error updating product list:', error.message);
    }
}

// 2. Push Fatigue 초기화
function truncatePushFatigue() {
    const query = 'TRUNCATE TABLE push_fatigue';
    executeQuery(query, [], 'Push Fatigue table truncated successfully.');
}

// 3. 연속 일수 초기화
function resetContinueNum() {
    const query = `
        UPDATE user SET continue_num = 0
        WHERE uid IN (
            SELECT tmp.uid FROM (
                SELECT u.uid, COUNT(a.uid) AS cnt 
                FROM user u
                LEFT JOIN activity a 
                ON u.uid = a.uid AND DATE(a.reg_dt) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                WHERE u.continue_num > 0
                GROUP BY u.uid
            ) tmp WHERE tmp.cnt = 0
        );
    `;
    executeQuery(query, [], 'Continue numbers reset successfully.');
}

// 4. 회원 정보 업데이트
function updateActivityNum() {
    const query = `
        UPDATE user a
        LEFT JOIN (
            SELECT a.uid, COUNT(DISTINCT b.id) AS activity_count
            FROM user a
            LEFT JOIN activity b
            ON a.uid = b.uid AND a.del_yn = 'N' AND b.del_yn = 'N'
            GROUP BY a.uid
        ) activity_counts ON a.uid = activity_counts.uid
        SET a.activity_num = COALESCE(activity_counts.activity_count, 0);
    `;
    executeQuery(query, [], 'Activity numbers updated successfully.');
}

// 5. 팔로우 및 팔로워 업데이트
function updateFollowNumbers() {
    const queries = [
        {
            query: `
                UPDATE user u
                LEFT JOIN (
                    SELECT uid, COUNT(DISTINCT following_uid) AS following_count
                    FROM follow f WHERE del_yn = 'N' GROUP BY uid
                ) follow_counts ON u.uid = follow_counts.uid
                SET u.following_num = COALESCE(follow_counts.following_count, 0);
            `,
            message: 'Following numbers updated successfully.'
        },
        {
            query: `
                UPDATE user u
                LEFT JOIN (
                    SELECT following_uid, COUNT(DISTINCT uid) AS follower_count
                    FROM follow f WHERE f.del_yn = 'N' GROUP BY following_uid
                ) follower_counts ON u.uid = follower_counts.following_uid
                SET u.follower_num = COALESCE(follower_counts.follower_count, 0);
            `,
            message: 'Follower numbers updated successfully.'
        }
    ];

    queries.forEach(item => executeQuery(item.query, [], item.message));
}

// 크론 작업 스케줄링
connectDB();
cron.schedule('0 0 * * *', truncatePushFatigue, { timezone: "Asia/Seoul" });
cron.schedule('0 0 * * *', resetContinueNum, { timezone: "Asia/Seoul" });
cron.schedule('0 4 * * *', updateActivityNum, { timezone: "Asia/Seoul" });
cron.schedule('15 4 * * *', updateFollowNumbers, { timezone: "Asia/Seoul" });
cron.schedule('50 22 * * *', updateProductList, { timezone: "Asia/Seoul" });

// 데이터베이스 연결 종료 처리
process.on('SIGINT', () => {
    connection.end(err => {
        if (err) console.error('Error disconnecting from the database:', err);
        console.log('Disconnected from the database');
        process.exit(0);
    });
});
