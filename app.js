const cron = require('node-cron');
const mysql = require('mysql');
const axios = require('axios');
const FormData = require('form-data'); // FormData가 필요할 경우

// MySQL 연결 설정
const dbConfig = {
    host: 'kkokiyo-mysql.c1kmsw8s42mh.ap-northeast-2.rds.amazonaws.com',
    port: '3306',
    user: 'root',
    password: 'kky240101!',
    database: 'kkokiyodb'
};

// DB 연결 생성 함수
function createConnection() {
    return mysql.createConnection(dbConfig);
}

// 쿼리 실행 함수 (연결 객체를 매개변수로 받음)
function executeQuery(connection, query, params = [], successMessage = '') {
    return new Promise((resolve, reject) => {
        connection.query(query, params, (err, results) => {
            if (err) {
                console.error('Error executing query:', err);
                return reject(err);
            } else {
                if (successMessage) console.log(successMessage, results);
                return resolve(results);
            }
        });
    });
}

// 1. 상품 리스트 저장 작업
async function updateProductList() {
    const connection = createConnection();
    connection.connect(err => {
        if (err) {
            console.error('Error connecting to the database:', err);
            return;
        }
        console.log('Connected to the database');
    });

    try {
        console.log('Starting product list update...');
        await executeQuery(connection, 'TRUNCATE TABLE giftshow_products', [], 'Products table truncated.');

        const API_URL = 'https://bizapi.giftishow.com/bizApi/goods';
        const AUTH_CODE = 'REALb2f7bab33e0242928af994d65edbd461';
        const AUTH_TOKEN = 'Hl7xPtrvSb6XEdvdY7pOBA==';

        // 요청 데이터를 FormData로 구성
        const d = new FormData();
        d.append("api_code", "0101");
        d.append("custom_auth_code", AUTH_CODE);
        d.append("custom_auth_token", AUTH_TOKEN);
        d.append("dev_yn", "N");
        d.append("start", "1");
        d.append("size", "2000");

        console.log('Request Data:', {
            api_code: "0101",
            custom_auth_code: AUTH_CODE,
            custom_auth_token: AUTH_TOKEN,
            dev_yn: "N",
            start: "1",
            size: "2000"
        });

        const response = await axios.post(API_URL, d);

        // 결과 검증
        if (!response.data || response.data.code !== '0000' || !response.data.result) {
            throw new Error(`Invalid API response: ${JSON.stringify(response.data)}`);
        }

        const products = response.data.result.goodsList;

        if (!products || products.length === 0) {
            console.warn('No products found in the API response.');
            return;
        }

        const insertQuery = `
            INSERT INTO giftshow_products 
            (rmIdBuyCntFlagCd, discountRate, mdCode, endDate, discountPrice, 
             mmsGoodsImg, srchKeyword, limitDay, content, goodsImgB, goodsTypeNm, 
             validPrdDay, goodsComName, goodsName, mmsReserveFlag, goodsStateCd, 
             brandCode, goodsNo, brandName, mmsBarcdCreateYn, salePrice, brandIconImg, goodsComId, 
             affiliateId, rmCntFlag, goodsCode, goodsTypeDtlNm, category1Seq, 
             goodsImgS, affiliate, validPrdTypeCd, saleDateFlag, realPrice)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `;

        for (const product of products) {
            await executeQuery(connection, insertQuery, [
                product.rmIdBuyCntFlagCd, product.discountRate, product.mdCode,
                product.endDate, product.discountPrice, product.mmsGoodsImg, product.srchKeyword,
                product.limitDay, product.content, product.goodsImgB, 
                product.goodsTypeNm, product.validPrdDay, product.goodsComName,
                product.goodsName, product.mmsReserveFlag, product.goodsStateCd,
                product.brandCode, product.goodsNo, product.brandName, 
                product.mmsBarcdCreateYn, product.salePrice, product.brandIconImg, 
                product.goodsComId, product.affiliateId, product.rmCntFlag,
                product.goodsCode, product.goodsTypeDtlNm, product.category1Seq, 
                product.goodsImgS, product.affiliate, product.validPrdTypeCd,
                product.saleDateFlag, product.realPrice
            ]);
        }
        console.log('Product list updated successfully.');
    } catch (error) {
        console.error('Error updating product list:', error.message);
    } finally {
        connection.end(err => {
            if (err) console.error('Error disconnecting from the database:', err);
            else console.log('Disconnected from the database');
        });
    }
}

// 2. Push Fatigue 초기화 작업
async function truncatePushFatigue() {
    const connection = createConnection();
    connection.connect(err => {
        if (err) {
            console.error('Error connecting to the database:', err);
            return;
        }
        console.log('Connected to the database');
    });

    try {
        const query = 'TRUNCATE TABLE push_fatigue';
        await executeQuery(connection, query, [], 'Push Fatigue table truncated successfully.');
    } catch (error) {
        console.error('Error truncating push fatigue:', error.message);
    } finally {
        connection.end(err => {
            if (err) console.error('Error disconnecting from the database:', err);
            else console.log('Disconnected from the database');
        });
    }
}

// 3. 연속 일수 초기화 작업
async function resetContinueNum() {
    const connection = createConnection();
    connection.connect(err => {
        if (err) {
            console.error('Error connecting to the database:', err);
            return;
        }
        console.log('Connected to the database');
    });

    try {
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
        await executeQuery(connection, query, [], 'Continue numbers reset successfully.');
    } catch (error) {
        console.error('Error resetting continue numbers:', error.message);
    } finally {
        connection.end(err => {
            if (err) console.error('Error disconnecting from the database:', err);
            else console.log('Disconnected from the database');
        });
    }
}

// 4. 회원 정보 업데이트 작업
async function updateActivityNum() {
    const connection = createConnection();
    connection.connect(err => {
        if (err) {
            console.error('Error connecting to the database:', err);
            return;
        }
        console.log('Connected to the database');
    });

    try {
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
        await executeQuery(connection, query, [], 'Activity numbers updated successfully.');
    } catch (error) {
        console.error('Error updating activity numbers:', error.message);
    } finally {
        connection.end(err => {
            if (err) console.error('Error disconnecting from the database:', err);
            else console.log('Disconnected from the database');
        });
    }
}

// 5. 팔로우 및 팔로워 업데이트 작업
async function updateFollowNumbers() {
    const connection = createConnection();
    connection.connect(err => {
        if (err) {
            console.error('Error connecting to the database:', err);
            return;
        }
        console.log('Connected to the database');
    });

    try {
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

        for (const item of queries) {
            await executeQuery(connection, item.query, [], item.message);
        }
    } catch (error) {
        console.error('Error updating follow numbers:', error.message);
    } finally {
        connection.end(err => {
            if (err) console.error('Error disconnecting from the database:', err);
            else console.log('Disconnected from the database');
        });
    }
}

// 6. activity 좋아요수 업데이트 작업
async function updateActivityLikeNum() {
    const connection = createConnection();
    connection.connect(err => {
        if (err) {
            console.error('Error connecting to the database:', err);
            return;
        }
        console.log('Connected to the database');
    });

    try {
        const query = `
            UPDATE activity a
            LEFT JOIN (
                SELECT a.id, COUNT(DISTINCT b.id) AS like_count
                FROM activity a 
                LEFT JOIN activity_like b
                ON a.id = b.activity_id AND a.del_yn = 'N' AND b.del_yn = 'N'
                GROUP BY a.id          
            ) like_counts ON a.id = like_counts.id
            SET a.like_cnt = COALESCE(like_counts.like_count, 0)
        `;
        await executeQuery(connection, query, [], 'Activity Like numbers updated successfully.');
    } catch (error) {
        console.error('Error updating activity like numbers:', error.message);
    } finally {
        connection.end(err => {
            if (err) console.error('Error disconnecting from the database:', err);
            else console.log('Disconnected from the database');
        });
    }
}

// 7. activity reply num 업데이트 작업
async function updateActivityReplyNum() {
    const connection = createConnection();
    connection.connect(err => {
        if (err) {
            console.error('Error connecting to the database:', err);
            return;
        }
        console.log('Connected to the database');
    });

    try {
        const query = `
            UPDATE activity a
            LEFT JOIN (
                SELECT a.id, COUNT(DISTINCT b.id) AS reply_count
                FROM activity a 
                LEFT JOIN reply b
                ON a.id = b.activity_id AND a.del_yn = 'N' AND b.del_yn = 'N'
                GROUP BY a.id          
            ) reply_counts ON a.id = reply_counts.id
            SET a.reply_cnt = COALESCE(reply_counts.reply_count, 0)
        `;
        await executeQuery(connection, query, [], 'Activity reply numbers updated successfully.');
    } catch (error) {
        console.error('Error updating activity reply numbers:', error.message);
    } finally {
        connection.end(err => {
            if (err) console.error('Error disconnecting from the database:', err);
            else console.log('Disconnected from the database');
        });
    }
}

// 8. content 좋아요수 업데이트 작업
async function updateContentLikeNum() {
    const connection = createConnection();
    connection.connect(err => {
        if (err) {
            console.error('Error connecting to the database:', err);
            return;
        }
        console.log('Connected to the database');
    });

    try {
        const query = `
            UPDATE content a
            LEFT JOIN (
                SELECT a.id, COUNT(DISTINCT b.uid) AS like_count
                FROM content a 
                LEFT JOIN content_like b
                ON a.id = b.content_id AND a.del_yn = 'N' AND b.del_yn = 'N'
                GROUP BY a.id          
            ) like_counts ON a.id = like_counts.id
            SET a.like_cnt = COALESCE(like_counts.like_count, 0);
        `;
        await executeQuery(connection, query, [], 'Content Like numbers updated successfully.');
    } catch (error) {
        console.error('Error updating content like numbers:', error.message);
    } finally {
        connection.end(err => {
            if (err) console.error('Error disconnecting from the database:', err);
            else console.log('Disconnected from the database');
        });
    }
}

// 9. 상품 삭제 작업
async function updateGoods() {
    const connection = createConnection();
    connection.connect(err => {
        if (err) {
            console.error('Error connecting to the database:', err);
            return;
        }
        console.log('Connected to the database');
    });

    try {
        const query = `
            UPDATE goods_giftshow_2 a
            INNER JOIN (
                SELECT a.goods_code
                FROM goods_giftshow_2 a
                where a.goods_code not in (select goodsCode from giftshow_products gp)
            ) AS t ON a.goods_code = t.goods_code
            SET a.sale_yn = 'N';
        `;
        await executeQuery(connection, query, [], 'Goods updated successfully.');
    } catch (error) {
        console.error('Error updating Goods:', error.message);
    } finally {
        connection.end(err => {
            if (err) console.error('Error disconnecting from the database:', err);
            else console.log('Disconnected from the database');
        });
    }
}

// 10. 신규 상품 Insert
async function insertGoods() {
    const connection = createConnection();
    connection.connect(err => {
        if (err) {
            console.error('Error connecting to the database:', err);
            return;
        }
        console.log('Connected to the database');
    });

    try {
        const query = `
            INSERT INTO goods_giftshow_2
            (goods_name, goods_code, goods_img_s, goods_img_b, limit_day, sale_price, discount_price, content, brand_name, brand_icon_img, discount_rate, sale_yn, del_yn, reg_dt, mod_dt, del_dt, buy_cnt, order_num, category_id)
            select goodsName, goodsCode, goodsImgS, goodsImgB, limitDay, salePrice, discountPrice, content, brandName, brandIconImg, discountRate, 'N', 'N', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, 0, 0, 0
            from giftshow_products
            where goodsCode not in ( select goods_code from goods_giftshow_2 )        
            ;
        `;
        await executeQuery(connection, query, [], 'Goods insert successfully.');
    } catch (error) {
        console.error('Error updating Goods:', error.message);
    } finally {
        connection.end(err => {
            if (err) console.error('Error disconnecting from the database:', err);
            else console.log('Disconnected from the database');
        });
    }
}

// 크론 작업 스케줄링 (각 작업이 실행될 때마다 개별 DB 연결 생성/종료)
cron.schedule('0 0 * * *', truncatePushFatigue, { timezone: "Asia/Seoul" });
cron.schedule('5 0 * * *', resetContinueNum, { timezone: "Asia/Seoul" });
cron.schedule('0 1 * * *', updateProductList, { timezone: "Asia/Seoul" });
cron.schedule('0 2 * * *', updateGoods, { timezone: "Asia/Seoul" });
cron.schedule('30 2 * * *', insertGoods, { timezone: "Asia/Seoul" });
cron.schedule('0 3 * * *', updateActivityLikeNum, { timezone: "Asia/Seoul" });
cron.schedule('15 3 * * *', updateActivityReplyNum, { timezone: "Asia/Seoul" });
cron.schedule('30 3 * * *', updateContentLikeNum, { timezone: "Asia/Seoul" });
cron.schedule('0 4 * * *', updateActivityNum, { timezone: "Asia/Seoul" });
cron.schedule('15 4 * * *', updateFollowNumbers, { timezone: "Asia/Seoul" });

// 프로세스 종료 시 별도 처리 (각 작업 내에서 연결 종료하므로 별도 글로벌 연결 종료는 필요 없음)
process.on('SIGINT', () => {
    console.log('Process interrupted. Exiting.');
    process.exit(0);
});
