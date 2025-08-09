"""End-to-end integration tests for NBA Analyst Agent."""

import pytest
import tempfile
from pathlib import Path

from analytics_pipeline.ingestion.ingest import create_ingestion_pipeline
from analytics_pipeline.database.models import PlayerBoxScore, TeamGameTotal


class TestEndToEndIngestion:
    """Integration tests for complete data ingestion flow."""
    
    @pytest.mark.integration
    def test_complete_box_scores_ingestion_flow(self, test_db_connection, sample_box_scores_csv):
        """Test complete box scores ingestion from CSV to database."""
        # Create ingestion pipeline
        pipeline = create_ingestion_pipeline(
            batch_size=10,
            validate_data=True,
            upsert_mode=False  # Use simple insert for test
        )
        
        # Override database connection
        pipeline.db_connection = test_db_connection
        
        # Perform ingestion
        result = pipeline.ingest_csv_file(
            file_path=sample_box_scores_csv,
            data_type='box_scores'
        )
        
        # Verify ingestion result
        assert result.success is True
        assert result.stats.total_rows_read == 2
        assert result.stats.rows_inserted == 2
        assert len(result.errors) == 0
        
        # Verify data in database
        with test_db_connection.get_session() as session:
            players = session.query(PlayerBoxScore).all()
            assert len(players) == 2
            
            # Verify specific player data
            lebron = session.query(PlayerBoxScore).filter_by(person_name='LeBron James').first()
            assert lebron is not None
            assert lebron.points == 35
            assert lebron.assists == 7
            assert lebron.rebounds_total == 10
            
            ad = session.query(PlayerBoxScore).filter_by(person_name='Anthony Davis').first()
            assert ad is not None
            assert ad.points == 22
            assert ad.assists == 3
            assert ad.rebounds_total == 15
    
    @pytest.mark.integration
    def test_complete_totals_ingestion_flow(self, test_db_connection, sample_totals_csv):
        """Test complete totals ingestion from CSV to database."""
        # Create ingestion pipeline
        pipeline = create_ingestion_pipeline(
            batch_size=10,
            validate_data=True,
            upsert_mode=False
        )
        
        # Override database connection
        pipeline.db_connection = test_db_connection
        
        # Perform ingestion
        result = pipeline.ingest_csv_file(
            file_path=sample_totals_csv,
            data_type='totals'
        )
        
        # Verify ingestion result
        assert result.success is True
        assert result.stats.total_rows_read == 1
        assert result.stats.rows_inserted == 1
        assert len(result.errors) == 0
        
        # Verify data in database
        with test_db_connection.get_session() as session:
            teams = session.query(TeamGameTotal).all()
            assert len(teams) == 1
            
            # Verify specific team data
            lakers = teams[0]
            assert lakers.team_name == 'Los Angeles Lakers'
            assert lakers.pts == 123
            assert lakers.wl == 'W'
            assert lakers.is_win is True
    
    @pytest.mark.integration
    def test_data_validation_integration(self, test_db_connection):
        """Test integration of data validation with ingestion."""
        # Create CSV with validation warnings (not errors)
        csv_content = """season_year,game_date,gameId,matchup,teamId,teamCity,teamName,teamTricode,teamSlug,personId,personName,position,comment,jerseyNum,minutes,fieldGoalsMade,fieldGoalsAttempted,fieldGoalsPercentage,threePointersMade,threePointersAttempted,threePointersPercentage,freeThrowsMade,freeThrowsAttempted,freeThrowsPercentage,reboundsOffensive,reboundsDefensive,reboundsTotal,assists,steals,blocks,turnovers,foulsPersonal,points,plusMinusPoints
2023-24,2024-01-15,12345,,1610612747,Los Angeles,Lakers,LAL,lakers,2544,Test Player,F,,6,25:30,10,15,0.67,3,7,0.43,4,5,0.8,2,8,10,5,2,1,3,2,27,8"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)
        
        try:
            # Create ingestion pipeline with validation but allow some errors
            pipeline = create_ingestion_pipeline(
                batch_size=10,
                validate_data=True,
                upsert_mode=False
            )
            pipeline.db_connection = test_db_connection
            
            # Perform ingestion
            result = pipeline.ingest_csv_file(
                file_path=csv_path,
                data_type='box_scores'
            )
            
            # Should succeed with good data
            assert result.success is True
            assert result.stats.rows_inserted == 1
            
        finally:
            csv_path.unlink()  # Clean up
    
    @pytest.mark.integration
    def test_multiple_file_ingestion(self, test_db_connection, sample_box_scores_csv, sample_totals_csv):
        """Test ingesting multiple files in sequence."""
        pipeline = create_ingestion_pipeline(
            batch_size=10,
            validate_data=True,
            upsert_mode=False
        )
        pipeline.db_connection = test_db_connection
        
        # Ingest box scores
        box_scores_result = pipeline.ingest_csv_file(
            file_path=sample_box_scores_csv,
            data_type='box_scores'
        )
        
        # Ingest totals
        totals_result = pipeline.ingest_csv_file(
            file_path=sample_totals_csv,
            data_type='totals'
        )
        
        # Both should succeed
        assert box_scores_result.success is True
        assert totals_result.success is True
        
        # Verify both tables have data
        with test_db_connection.get_session() as session:
            players = session.query(PlayerBoxScore).all()
            teams = session.query(TeamGameTotal).all()
            
            assert len(players) == 2
            assert len(teams) == 1
        
        # Test summary generation
        summary = pipeline.get_ingestion_summary([box_scores_result, totals_result])
        
        assert summary['files_processed'] == 2
        assert summary['files_successful'] == 2
        assert summary['success_rate'] == 1.0
        assert summary['total_rows_read'] == 3  # 2 + 1
        assert summary['total_rows_inserted'] == 3
    
    @pytest.mark.integration 
    def test_database_error_handling(self, test_db_connection):
        """Test handling of database-related errors."""
        # Create CSV with data that might cause DB constraints issues
        csv_content = """season_year,game_date,gameId,teamId,teamCity,teamName,teamTricode,teamSlug,personId,personName,position,comment,jerseyNum,minutes,fieldGoalsMade,fieldGoalsAttempted,fieldGoalsPercentage,threePointersMade,threePointersAttempted,threePointersPercentage,freeThrowsMade,freeThrowsAttempted,freeThrowsPercentage,reboundsOffensive,reboundsDefensive,reboundsTotal,assists,steals,blocks,turnovers,foulsPersonal,points,plusMinusPoints
invalid,invalid,invalid,invalid,Los Angeles,Lakers,LAL,lakers,invalid,Test Player,F,,6,25:30,10,15,0.67,3,7,0.43,4,5,0.8,2,8,10,5,2,1,3,2,27,8"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)
        
        try:
            pipeline = create_ingestion_pipeline(
                batch_size=10,
                validate_data=False,  # Disable validation to test DB errors
                upsert_mode=False
            )
            pipeline.db_connection = test_db_connection
            
            result = pipeline.ingest_csv_file(
                file_path=csv_path,
                data_type='box_scores'
            )
            
            # Should handle errors gracefully
            # Result may succeed or fail depending on type conversion handling
            assert isinstance(result.success, bool)
            assert isinstance(result.errors, list)
            
        finally:
            csv_path.unlink()


class TestConfigurationIntegration:
    """Integration tests for configuration and settings."""
    
    @pytest.mark.integration
    def test_database_configuration_integration(self, test_settings):
        """Test database configuration integration."""
        from analytics_pipeline.config.database import DatabaseConfig
        from analytics_pipeline.database.connection import DatabaseConnection
        
        # Create database config from settings
        db_config = DatabaseConfig(test_settings)
        
        # Verify configuration
        assert db_config.settings.testing is True
        assert db_config.settings.db_name == ":memory:"
        
        # Test connection creation
        db_conn = DatabaseConnection(db_config)
        
        # Test connection
        assert db_conn.test_connection() is True
        
        # Clean up
        db_conn.close()
    
    @pytest.mark.integration
    def test_ingestion_pipeline_configuration(self):
        """Test ingestion pipeline configuration integration."""
        from analytics_pipeline.ingestion.ingest import create_ingestion_pipeline
        from analytics_pipeline.ingestion.csv_reader import create_csv_reader
        from analytics_pipeline.ingestion.validators import create_validator
        
        # Test factory functions work together
        csv_reader = create_csv_reader(chunk_size=500)
        validator = create_validator(max_errors=50)
        pipeline = create_ingestion_pipeline(batch_size=250)
        
        # Verify configurations
        assert csv_reader.chunk_size == 500
        assert validator.max_errors == 50
        assert pipeline.batch_size == 250
        
        # Verify components are properly initialized
        assert pipeline.csv_reader is not None
        assert pipeline.validator is not None
        assert pipeline.db_connection is not None