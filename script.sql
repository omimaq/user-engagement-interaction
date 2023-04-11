--AUTHOR: OSAMA AKBAR QURESHI
--CREATED DATE: 2020-04-21
--LIFECYCLE: 36000 DAYS
--DESCRIPTION: USER SESSIONS INTERACTION WITH GA CLIENT DAILY INCREMENTAL PERFORMANCE TABLE
INSERT INTO
   `project.user_data.user_interaction ` ( date, country_code, web_page, clientId, channelGrouping, new_visitors_flag, landing_page_session_id, landing_page_post_id, landing_page_url, landing_page_category, landing_page_avg_time_on_page, interaction_1_session_id, interaction_1_post_id, interaction_1_url, interaction_1_category, interaction_1_avg_time_on_page, interaction_2_session_id, interaction_2_post_id, interaction_2_url, interaction_2_category, interaction_2_avg_time_on_page, interaction_3_session_id, interaction_3_post_id, interaction_3_url, interaction_3_category, interaction_3_avg_time_on_page, interaction_4_session_id, interaction_4_post_id, interaction_4_url, interaction_4_category, interaction_4_avg_time_on_page, interaction_5_session_id, interaction_5_post_id, interaction_5_url, interaction_5_category, interaction_5_avg_time_on_page) ## INTERACTION LEVEL - 2 
   SELECT
      date,
      country_code,
      web_page,
      clientId,
      channelGrouping,
      new_visitors_flag,
      landing_page_session_id,
      landing_page_post_id,
      landing_page_url,
      landing_page_category,
      landing_page_avg_time_on_page,
      interaction_1_session_id,
      interaction_1_post_id,
      interaction_1_url,
      interaction_1_category,
      interaction_1_avg_time_on_page,
      interaction_2_session_id,
      interaction_2_post_id,
      interaction_2_url,
      interaction_2_category,
      interaction_2_avg_time_on_page,
      interaction_3_session_id,
      interaction_3_post_id,
      interaction_3_url,
      interaction_3_category,
      interaction_3_avg_time_on_page,
      interaction_4_session_id,
      interaction_4_post_id,
      interaction_4_url,
      interaction_4_category,
      interaction_4_avg_time_on_page,
      interaction_5_session_id,
      interaction_5_post_id,
      interaction_5_url,
      interaction_5_category,
      interaction_5_avg_time_on_page 
   FROM
      (
         ## INTERACTION LEVEL - 1 
         SELECT
            date,
            country_code,
            web_page,
            session_id,
            clientId,
            channelGrouping,
            # INTERACTION - 1 MAX(IF(interaction = 1, session_id, Null)) AS landing_page_session_id,
            MAX(IF(interaction = 1, post_id, Null)) AS landing_page_post_id,
            CASE
               WHEN
                  MAX(IF(interaction = 1, url, Null)) IS NULL 
               THEN
                  'exit' 
               ELSE
                  MAX(IF(interaction = 1, url, Null)) 
            END
            AS landing_page_url, MAX(IF(interaction = 1, category, Null)) AS landing_page_category, MAX(IF(interaction = 1, avg_time_on_page, Null)) AS landing_page_avg_time_on_page, new_visitors_flag, # INTERACTION - 2 MAX(IF(interaction = 2, session_id, Null)) AS interaction_1_session_id, MAX(IF(interaction = 2, post_id, Null)) AS interaction_1_post_id, 
            CASE
               WHEN
                  MAX(IF(interaction = 2, url, Null)) IS NULL 
               THEN
                  'exit' 
               ELSE
                  MAX(IF(interaction = 2, url, Null)) 
            END
            AS interaction_1_url, MAX(IF(interaction = 2, category, Null)) AS interaction_1_category, MAX(IF(interaction = 2, avg_time_on_page, Null)) AS interaction_1_avg_time_on_page, # INTERACTION - 3 MAX(IF(interaction = 3, session_id, Null)) AS interaction_2_session_id, MAX(IF(interaction = 3, post_id, Null)) AS interaction_2_post_id, 
            CASE
               WHEN
                  MAX(IF(interaction = 3, url, Null)) IS NULL 
               THEN
                  'exit' 
               ELSE
                  MAX(IF(interaction = 3, url, Null)) 
            END
            AS interaction_2_url, MAX(IF(interaction = 3, category, Null)) AS interaction_2_category, MAX(IF(interaction = 3, avg_time_on_page, Null)) AS interaction_2_avg_time_on_page, # INTERACTION - 4 MAX(IF(interaction = 4, session_id, Null)) AS interaction_3_session_id, MAX(IF(interaction = 4, post_id, Null)) AS interaction_3_post_id, 
            CASE
               WHEN
                  MAX(IF(interaction = 4, url, Null)) IS NULL 
               THEN
                  'exit' 
               ELSE
                  MAX(IF(interaction = 4, url, Null)) 
            END
            AS interaction_3_url, MAX(IF(interaction = 4, category, Null)) AS interaction_3_category, MAX(IF(interaction = 4, avg_time_on_page, Null)) AS interaction_3_avg_time_on_page, # INTERACTION - 5 MAX(IF(interaction = 5, session_id, Null)) AS interaction_4_session_id, MAX(IF(interaction = 5, post_id, Null)) AS interaction_4_post_id, 
            CASE
               WHEN
                  MAX(IF(interaction = 5, url, Null)) IS NULL 
               THEN
                  'exit' 
               ELSE
                  MAX(IF(interaction = 5, url, Null)) 
            END
            AS interaction_4_url, MAX(IF(interaction = 5, category, Null)) AS interaction_4_category, MAX(IF(interaction = 5, avg_time_on_page, Null)) AS interaction_4_avg_time_on_page, # INTERACTION - 6 MAX(IF(interaction = 6, session_id, Null)) AS interaction_5_session_id, MAX(IF(interaction = 6, post_id, Null)) AS interaction_5_post_id, 
            CASE
               WHEN
                  MAX(IF(interaction = 6, url, Null)) IS NULL 
               THEN
                  'exit' 
               ELSE
                  MAX(IF(interaction = 6, url, Null)) 
            END
            AS interaction_5_url, MAX(IF(interaction = 6, category, Null)) AS interaction_5_category, MAX(IF(interaction = 6, avg_time_on_page, Null)) AS interaction_5_avg_time_on_page 
         FROM
            (
               #### FINAL LEVEL 
               SELECT
                  ga_sessions.date,
                  ga_sessions.country_code,
                  ga_sessions.web_page,
                  ga_sessions.clientId,
                  ga_sessions.channelGrouping,
                  ga_sessions.session_id,
                  ga_sessions.post_id,
                  ga_sessions.url,
                  ga_sessions.rank AS interaction,
                  CASE
                     WHEN
                        cms_df.category_cms IS NULL 
                     THEN
                        cat_df.category_cms_cat 
                     ELSE
                        cms_df.category_cms 
                  END
                  AS category, ga_sessions.new_visitors_flag, 
                  CASE
                     WHEN
                        pageviews = exits 
                     THEN
                        0 
                     ELSE
                        time_on_page / (pageviews - exits) 
                  END
                  AS avg_time_on_page 
               FROM
                  (
                     #### level - 1 (GA_SESSIONS) 
                     SELECT
                        date,
                        country_code,
                        web_page,
                        clientId,
                        channelGrouping,
                        session_id,
                        post_id,
                        url,
                        RANK() OVER (PARTITION BY session_id 
                     ORDER BY
                        hitTimeUtc ASC, time ASC, hitNumber ASC) AS rank,
                        newVisits AS new_visitors_flag,
                        ## 
                        CASE
                           WHEN
                              newVisits IS NULL 
                           THEN
                              1 
                           ELSE
                              0 
                        END
                        AS return_visitors_flag, 
                        CASE
                           WHEN
                              type = 'PAGE' 
                           THEN
                              1 
                        END
                        AS pageviews, 
                        CASE
                           WHEN
                              type = 'PAGE' 
                           THEN
                              exits 
                        END
                        AS exits, 
                        CASE
                           WHEN
                              isExit IS NOT NULL 
                           THEN
                              last_interaction_time - time 
                           ELSE
                              next_pageview_time - time 
                        END
                        AS time_on_page 
                     FROM
                        `project.dataset.ga_sessions` 
                     WHERE
                        date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
                        AND type = 'PAGE' 
                  )
                  ga_sessions #### 
                  LEFT JOIN
                     CMS 
                  LEFT OUTER JOIN
                     (
                        SELECT
                           date,
                           MAX(post_id) AS post_id,
                           country_code,
                           url,
                           web_page,
                           CASE
                              WHEN
                                 (
(LENGTH(category_2_en) = 0 
                                    OR category_2_en IS NULL) 
                                    AND 
                                    (
                                       LENGTH(category_3_en) = 0 
                                       OR category_3_en IS NULL
                                    )
                                 )
                              THEN
                                 category_1_en 
                              WHEN
                                 (
(LENGTH(category_2_en) > 0 
                                    OR category_2_en IS NOT NULL) 
                                    AND 
                                    (
                                       LENGTH(category_3_en) = 0 
                                       OR category_3_en IS NULL
                                    )
                                 )
                              THEN
                                 category_2_en 
                              WHEN
                                 (
                                    LENGTH(category_3_en) > 0 
                                    OR category_3_en IS NOT NULL
                                 )
                              THEN
                                 category_3_en 
                           END
                           AS category_cms, category_1_en, category_2_en, category_3_en 
                        FROM
                           `project.dataset.cms_data` 
                        WHERE
                           date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
                           AND post_status IN 
                           (
                              'publish', 'future'
                           )
                        GROUP BY
                           date, country_code, url, web_page, category_1_en, category_2_en, category_3_en
                     )
                     cms_df 
                     ON cms_df.date = ga_sessions.date 
                     AND cms_df.post_id = ga_sessions.post_id 
                     AND cms_df.web_page = ga_sessions.web_page 
                     AND cms_df.country_code = ga_sessions.country_code ##### 
                  LEFT JOIN
                     CMS CATEGORY 
                  LEFT OUTER JOIN
                     (
                        SELECT
                           date,
                           country_code,
                           url,
                           web_page,
                           CASE
                              WHEN
                                 (
(LENGTH(category_2_en) = 0 
                                    OR category_2_en IS NULL) 
                                    AND 
                                    (
                                       LENGTH(category_3_en) = 0 
                                       OR category_3_en IS NULL
                                    )
                                 )
                              THEN
                                 category_1_en 
                              WHEN
                                 (
(LENGTH(category_2_en) > 0 
                                    OR category_2_en IS NOT NULL) 
                                    AND 
                                    (
                                       LENGTH(category_3_en) = 0 
                                       OR category_3_en IS NULL
                                    )
                                 )
                              THEN
                                 category_2_en 
                              WHEN
                                 (
                                    LENGTH(category_3_en) > 0 
                                    OR category_3_en IS NOT NULL
                                 )
                              THEN
                                 category_3_en 
                           END
                           AS category_cms_cat 
                        FROM
                           `project.dataset.cms_category_data` 
                        WHERE
                           date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
                        GROUP BY
                           date, country_code, url, web_page, category_1_en, category_2_en, category_3_en
                     )
                     cat_df 
                     ON cat_df.date = ga_sessions.date 
                     AND cat_df.url = ga_sessions.url 
                     AND cat_df.web_page = ga_sessions.web_page 
                     AND cat_df.country_code = ga_sessions.country_code 
            )
         GROUP BY
            date, country_code, web_page, session_id, clientId, channelGrouping, new_visitors_flag
      )
   WHERE
      landing_page_url != interaction_1_url 
      AND landing_page_url != interaction_2_url 
      AND landing_page_url != interaction_3_url 
      AND landing_page_url != interaction_4_url 
      AND landing_page_url != interaction_5_url 
      AND web_page is not null 
      AND landing_page_session_id IS NOT NULL