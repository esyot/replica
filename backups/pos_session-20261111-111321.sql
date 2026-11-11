-- MySQL dump 10.13  Distrib 8.4.11, for Linux (x86_64)
--
-- Host: 127.0.0.1    Database: candg_pos_db
-- ------------------------------------------------------
-- Server version	5.5.5-10.4.32-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Dumping data for table `pos_session`
--

LOCK TABLES `pos_session` WRITE;
/*!40000 ALTER TABLE `pos_session` DISABLE KEYS */;
INSERT INTO `pos_session` (`record_id`, `user`, `store`, `opening_balance`, `closing_balance`, `total_amount_sale`, `opened_at`, `closed_at`, `status`, `synced_at`, `created_at`, `updated_at`) VALUES ('29880cee-4575-4b0b-83f3-f802192acbfb','6c555903-1662-408d-bc85-31744fa882da','44d405ba-fec6-968c-fcb4-3724ada2829a',1000.00,NULL,NULL,'2026-11-10 23:56:00',NULL,'1',NULL,'2026-11-10 15:56:41','2026-11-10 15:56:41'),('391b690f-11d4-4b89-a75a-6c5e2d21f40a','72c4a43a-4ef5-425b-bec7-17d2021f13cd','c57f0070-0853-91f0-893f-497985f3f7ea',0.00,NULL,NULL,'2026-11-09 16:00:00',NULL,'1',NULL,'2026-11-10 09:13:42','2026-11-10 09:13:42'),('54a38cf4-047f-4703-957e-3331febc94e1','239974a1-9985-40f0-a1cd-828f6d133650','44d405ba-fec6-968c-fcb4-3724ada2829a',1000.00,NULL,NULL,'2026-11-10 01:52:00',NULL,'1',NULL,'2026-11-10 17:52:46','2026-11-10 17:52:46'),('8f5eb9d1-041d-4c59-819f-851f55e0d7f3','6c555903-1662-408d-bc85-31744fa882da','44d405ba-fec6-968c-fcb4-3724ada2829a',1000.00,NULL,NULL,'2026-11-10 01:50:00',NULL,'1',NULL,'2026-11-10 17:50:49','2026-11-10 17:50:49');
/*!40000 ALTER TABLE `pos_session` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `sym_trig_pos_session_ins`
    AFTER INSERT ON `pos_session`
    FOR EACH ROW
    BEGIN
      IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES ('pos_session', 'INSERT', CAST(NEW.`record_id` AS CHAR), JSON_OBJECT('record_id', NEW.`record_id`, 'user', NEW.`user`, 'store', NEW.`store`, 'opening_balance', NEW.`opening_balance`, 'closing_balance', NEW.`closing_balance`, 'total_amount_sale', NEW.`total_amount_sale`, 'opened_at', NEW.`opened_at`, 'closed_at', NEW.`closed_at`, 'status', NEW.`status`, 'synced_at', NEW.`synced_at`, 'created_at', DATE_FORMAT(NEW.`created_at`, '%Y-%m-%d %H:%i:%s'), 'updated_at', DATE_FORMAT(NEW.`updated_at`, '%Y-%m-%d %H:%i:%s')), 'NODE_MAIN_HUB');
      END IF;
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `sym_trig_pos_session_upd`
    AFTER UPDATE ON `pos_session`
    FOR EACH ROW
    BEGIN
      IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES ('pos_session', 'UPDATE', CAST(NEW.`record_id` AS CHAR), JSON_OBJECT('record_id', NEW.`record_id`, 'user', NEW.`user`, 'store', NEW.`store`, 'opening_balance', NEW.`opening_balance`, 'closing_balance', NEW.`closing_balance`, 'total_amount_sale', NEW.`total_amount_sale`, 'opened_at', NEW.`opened_at`, 'closed_at', NEW.`closed_at`, 'status', NEW.`status`, 'synced_at', NEW.`synced_at`, 'created_at', DATE_FORMAT(NEW.`created_at`, '%Y-%m-%d %H:%i:%s'), 'updated_at', DATE_FORMAT(NEW.`updated_at`, '%Y-%m-%d %H:%i:%s')), 'NODE_MAIN_HUB');
      END IF;
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `sym_trig_pos_session_del`
    AFTER DELETE ON `pos_session`
    FOR EACH ROW
    BEGIN
      IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES ('pos_session', 'DELETE', CAST(OLD.`record_id` AS CHAR), JSON_OBJECT('record_id', OLD.`record_id`, 'user', OLD.`user`, 'store', OLD.`store`, 'opening_balance', OLD.`opening_balance`, 'closing_balance', OLD.`closing_balance`, 'total_amount_sale', OLD.`total_amount_sale`, 'opened_at', OLD.`opened_at`, 'closed_at', OLD.`closed_at`, 'status', OLD.`status`, 'synced_at', OLD.`synced_at`, 'created_at', DATE_FORMAT(OLD.`created_at`, '%Y-%m-%d %H:%i:%s'), 'updated_at', DATE_FORMAT(OLD.`updated_at`, '%Y-%m-%d %H:%i:%s')), 'NODE_MAIN_HUB');
      END IF;
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-11-11 11:13:21
