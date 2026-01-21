import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
import warnings
from collections import defaultdict
import logging

warnings.filterwarnings('ignore')

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('camera_imputation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CameraDataImputer:
    def __init__(self, db_path: str, target_client_locations: List[Tuple[str, str]] = None):
        """
        Sistema de imputação de dados de câmeras - VERSÃO CORRIGIDA 2.0.
        """
        self.db_path = db_path
        self.target_client_locations = target_client_locations
        self.conn = None
        
        # Mapeamento de dias da semana
        self.weekday_columns = {
            0: ('counting_hour_monday', 'counting_hour_monday_qtd'),
            1: ('counting_hour_tuesday', 'counting_hour_tuesday_qtd'),
            2: ('counting_hour_wednesday', 'counting_hour_wednesday_qtd'),
            3: ('counting_hour_thursday', 'counting_hour_thursday_qtd'),
            4: ('counting_hour_fryday', 'counting_hour_fryday_qtd'),
            5: ('counting_hour_saturday', 'counting_hour_saturday_qtd'),
            6: ('counting_hour_sunday', 'counting_hour_sunday_qtd'),
        }
        
        # Cache para melhor performance
        self.data_cache = {}
        
    def connect(self):
        """Estabelece conexão com o banco de dados."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn
        
    def disconnect(self):
        """Fecha a conexão com o banco de dados."""
        if self.conn:
            self.conn.close()
            self.conn = None
            
    def get_client_location_list(self) -> List[Tuple[str, str]]:
        """
        Obtém lista de pares cliente-localização para processar.
        """
        if self.target_client_locations:
            return self.target_client_locations
            
        self.connect()
        query = """
            SELECT DISTINCT client, location 
            FROM login_camera 
            WHERE client IS NOT NULL AND location IS NOT NULL
            AND client != '' AND location != ''
            ORDER BY client, location
        """
        df = pd.read_sql_query(query, self.conn)
        
        client_locations = list(df.itertuples(index=False, name=None))
        logger.info(f"Encontrados {len(client_locations)} pares cliente-localização")
        return client_locations
    
    def load_data_for_client_location(self, client: str, location: str, 
                                     days_back: int = 60) -> bool:
        """
        Carrega dados para um cliente-localização específico.
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Processando: {client} - {location}")
        logger.info(f"{'='*60}")
        
        self.connect()
        
        # Cache key
        cache_key = f"{client}_{location}_{days_back}"
        
        # Verificar cache primeiro
        if cache_key in self.data_cache:
            self.cameras_df, self.flow_df = self.data_cache[cache_key]
            logger.info(f"Dados carregados do cache para {client} - {location}")
            return True
        
        try:
            # Carrega câmeras
            camera_query = """
                SELECT 
                    id, client, location, pong_ts,
                    counting_hour_sunday, counting_hour_sunday_qtd,
                    counting_hour_monday, counting_hour_monday_qtd,
                    counting_hour_tuesday, counting_hour_tuesday_qtd,
                    counting_hour_wednesday, counting_hour_wednesday_qtd,
                    counting_hour_thursday, counting_hour_thursday_qtd,
                    counting_hour_fryday, counting_hour_fryday_qtd,
                    counting_hour_saturday, counting_hour_saturday_qtd,
                    counting_hour_holiday, counting_hour_holiday_qtd
                FROM login_camera
                WHERE client = ? AND location = ?
                AND id IS NOT NULL
            """
            
            self.cameras_df = pd.read_sql_query(camera_query, self.conn, params=[client, location])
            
            if self.cameras_df.empty:
                logger.warning(f"Nenhuma câmera encontrada para {client} - {location}")
                return False
                
            logger.info(f"Carregadas {len(self.cameras_df)} câmeras para {client} - {location}")
            
            # IDs das câmeras
            target_camera_ids = self.cameras_df['id'].tolist()
            
            # Data de corte
            cutoff_date = (datetime.now() - timedelta(days=max(days_back, 28))).strftime('%Y-%m-%d %H:%M:%S')
            
            # Carrega dados de fluxo
            if not target_camera_ids:
                logger.warning("Nenhuma câmera para carregar dados")
                return False
                
            placeholders = ','.join(['?'] * len(target_camera_ids))
            
            peopleflow_query = f"""
                SELECT 
                    id, created_at, camera_id, 
                    total_inside, total_outside, valid 
                FROM peopleflowtotals 
                WHERE created_at >= ? 
                AND camera_id IN ({placeholders})
                AND (valid = 1 OR valid IS NULL)
                ORDER BY created_at
            """
            
            peopleflow_params = [cutoff_date] + target_camera_ids
            
            self.flow_df = pd.read_sql_query(
                peopleflow_query, 
                self.conn, 
                params=peopleflow_params
            )
            
            if self.flow_df.empty:
                logger.warning(f"Nenhum dado de fluxo encontrado para {client} - {location}")
                return False
            
            # Processar colunas de data/hora
            self.flow_df['created_at'] = pd.to_datetime(self.flow_df['created_at'])
            self.flow_df['date'] = self.flow_df['created_at'].dt.date
            self.flow_df['hour'] = self.flow_df['created_at'].dt.hour
            self.flow_df['weekday'] = self.flow_df['created_at'].dt.weekday
            
            # Calcular tráfego total
            self.flow_df['total_traffic'] = self.flow_df['total_inside'] + self.flow_df['total_outside']
            
            # Filtrar apenas registros válidos para análise
            valid_mask = self.flow_df['valid'] == 1
            if valid_mask.any():
                self.flow_df_valid = self.flow_df[valid_mask].copy()
            else:
                self.flow_df_valid = self.flow_df.copy()
            
            # Estatísticas de dados carregados
            if not self.flow_df_valid.empty:
                min_date = self.flow_df_valid['date'].min()
                max_date = self.flow_df_valid['date'].max()
                date_range = (max_date - min_date).days + 1
                
                logger.info(f"Carregados {len(self.flow_df)} registros de fluxo")
                logger.info(f"Período: {min_date} a {max_date} ({date_range} dias)")
                logger.info(f"Registros válidos: {len(self.flow_df_valid)}")
            else:
                logger.warning("Nenhum registro válido encontrado")
                return False
            
            # Armazenar em cache
            self.data_cache[cache_key] = (self.cameras_df.copy(), self.flow_df.copy())
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {e}")
            return False
    
    def get_camera_active_hours(self, camera_id: int, weekday: int) -> Tuple[int, int]:
        """
        Obtém intervalo de horas ativas para uma câmera e dia da semana.
        """
        try:
            if camera_id not in self.cameras_df['id'].values:
                return (9, 18)  # Horário comercial padrão
            
            camera_row = self.cameras_df[self.cameras_df['id'] == camera_id].iloc[0]
            
            if weekday in self.weekday_columns:
                start_col, end_col = self.weekday_columns[weekday]
                start_hour = camera_row[start_col]
                end_hour = camera_row[end_col]
                
                # Tratar valores ausentes
                if pd.isna(start_hour) or pd.isna(end_hour):
                    return (9, 18)  # Fallback
                
                # Garantir valores válidos
                start_hour = max(0, min(23, int(start_hour)))
                end_hour = max(0, min(23, int(end_hour)))
                
                # Garantir que início < fim
                if start_hour > end_hour:
                    start_hour, end_hour = end_hour, start_hour
                
                return (start_hour, end_hour)
            
            return (9, 18)
            
        except Exception as e:
            logger.error(f"Erro ao obter horas ativas para câmera {camera_id}: {e}")
            return (9, 18)
    
    def get_last_valid_day(self) -> Optional[datetime]:
        """
        Obtém o último dia com dados válidos.
        """
        if not hasattr(self, 'flow_df_valid') or self.flow_df_valid.empty:
            logger.warning("Nenhum dado válido disponível")
            return None
        
        try:
            # Encontrar o último dia com dados
            last_date = self.flow_df_valid['date'].max()
            last_datetime = datetime.combine(last_date, datetime.min.time())
            
            logger.info(f"Último dia válido: {last_date}")
            return last_datetime
            
        except Exception as e:
            logger.error(f"Erro ao obter último dia válido: {e}")
            return None
    
    def is_holiday(self, date_obj: datetime) -> bool:
        """
        Verifica se uma data é feriado.
        """
        # Feriados fixos (exemplos - ajuste conforme necessidade)
        fixed_holidays = {
            (1, 1): "Ano Novo",
            (4, 21): "Tiradentes",
            (5, 1): "Dia do Trabalho",
            (9, 7): "Independência",
            (10, 12): "Nossa Senhora Aparecida",
            (11, 2): "Finados",
            (11, 15): "Proclamação da República",
            (12, 25): "Natal"
        }
        
        month_day = (date_obj.month, date_obj.day)
        return month_day in fixed_holidays
    
    def identify_failing_cameras(self, target_date: datetime = None) -> Dict[int, List[int]]:
        """
        Identifica câmeras com possíveis falhas.
        """
        try:
            if target_date is None:
                target_date = self.get_last_valid_day()
                
            if target_date is None:
                logger.warning("Nenhuma data alvo disponível")
                return {}
            
            target_date_str = target_date.strftime('%Y-%m-%d')
            target_weekday = target_date.weekday()
            
            logger.info(f"\nVerificando câmeras em {target_date_str} (dia {target_weekday})")
            
            # Obter dados do dia alvo
            target_data = self.flow_df_valid[
                (self.flow_df_valid['date'] == target_date.date())
            ]
            
            if target_data.empty:
                logger.warning(f"Nenhum dado para {target_date_str}")
                return {}
            
            # IDs de todas as câmeras
            all_camera_ids = self.cameras_df['id'].tolist()
            failing_cameras = {}
            
            # Análise por câmera
            for camera_id in all_camera_ids:
                try:
                    camera_failed_hours = []
                    start_hour, end_hour = self.get_camera_active_hours(camera_id, target_weekday)
                    
                    # Se não há horas ativas definidas, pular
                    if start_hour is None or end_hour is None:
                        continue
                    
                    active_hours = list(range(start_hour, end_hour + 1))
                    
                    if not active_hours:
                        continue
                    
                    logger.debug(f"Câmera {camera_id}: Horas ativas {start_hour}-{end_hour}")
                    
                    for hour in active_hours:
                        hour_target_data = target_data[
                            (target_data['camera_id'] == camera_id) &
                            (target_data['hour'] == hour)
                        ]
                        
                        if hour_target_data.empty:
                            # SEM DADOS - potencial falha
                            camera_failed_hours.append(hour)
                            logger.debug(f"  Hora {hour:02d}: SEM DADOS")
                            continue
                        
                        # Dados existentes - verificar se são muito baixos
                        row = hour_target_data.iloc[0]
                        current_inside = row['total_inside']
                        current_outside = row['total_outside']
                        current_total = current_inside + current_outside
                        
                        # Obter dados históricos para esta hora e dia
                        hist_data = self.flow_df_valid[
                            (self.flow_df_valid['camera_id'] == camera_id) &
                            (self.flow_df_valid['hour'] == hour) &
                            (self.flow_df_valid['weekday'] == target_weekday) &
                            (self.flow_df_valid['date'] < target_date.date())
                        ]
                        
                        if len(hist_data) >= 3:
                            hist_totals = hist_data['total_traffic']
                            hist_median = hist_totals.median()
                            hist_q1 = hist_totals.quantile(0.25)
                            hist_q3 = hist_totals.quantile(0.75)
                            
                            # Se valor atual for significativamente menor que a mediana
                            if current_total < (hist_median * 0.3) and hist_median > 10:
                                camera_failed_hours.append(hour)
                                logger.warning(f"  Hora {hour:02d}: BAIXO - {current_total} < 30% de {hist_median:.1f}")
                            elif current_total == 0 and hist_median > 5:
                                camera_failed_hours.append(hour)
                                logger.warning(f"  Hora {hour:02d}: ZERO - histórico mediana={hist_median:.1f}")
                            else:
                                logger.debug(f"  Hora {hour:02d}: OK - {current_total}")
                        else:
                            # Dados históricos insuficientes
                            logger.debug(f"  Hora {hour:02d}: Dados históricos insuficientes")
                            
                            # Se valor atual é zero em hora comercial, considerar falha
                            if current_total == 0 and 8 <= hour <= 20:
                                camera_failed_hours.append(hour)
                                logger.warning(f"  Hora {hour:02d}: ZERO em hora comercial sem histórico")
                    
                    # Registrar câmera se tiver horas com falha
                    if camera_failed_hours:
                        failing_cameras[camera_id] = camera_failed_hours
                        logger.info(f"Câmera {camera_id}: {len(camera_failed_hours)} horas com falha")
                        
                except Exception as e:
                    logger.error(f"Erro analisando câmera {camera_id}: {e}")
                    continue
            
            # Resumo
            logger.info(f"\n{'='*60}")
            logger.info(f"RESUMO: {len(failing_cameras)} câmeras com falha detectada")
            logger.info(f"{'='*60}")
            
            for camera_id, hours in failing_cameras.items():
                logger.info(f"  Câmera {camera_id}: horas com falha {sorted(hours)}")
            
            return failing_cameras
            
        except Exception as e:
            logger.error(f"Erro na identificação de falhas: {e}")
            return {}
    
    def estimate_missing_data(self, failing_cameras: Dict[int, List[int]], 
                            target_date: datetime) -> pd.DataFrame:
        """
        Estima dados ausentes - VERSÃO SIMPLIFICADA E CORRIGIDA.
        """
        try:
            logger.info(f"\nEstimando dados para {target_date.date()}...")
            
            if not failing_cameras:
                logger.info("Nenhuma câmera com falha para estimar")
                return pd.DataFrame()
            
            target_weekday = target_date.weekday()
            
            # Identificar câmeras funcionando
            all_camera_ids = self.cameras_df['id'].tolist()
            working_cameras = [cid for cid in all_camera_ids if cid not in failing_cameras]
            
            logger.info(f"Câmeras funcionando: {len(working_cameras)}")
            logger.info(f"Câmeras com falha: {len(failing_cameras)}")
            
            estimated_records = []
            
            for camera_id, missing_hours in failing_cameras.items():
                logger.info(f"\nProcessando Câmera {camera_id}")
                
                # Obter horas ativas
                start_hour, end_hour = self.get_camera_active_hours(camera_id, target_weekday)
                
                # Filtrar apenas horas dentro do período ativo
                if start_hour is not None and end_hour is not None:
                    active_hours_set = set(range(start_hour, end_hour + 1))
                    missing_hours = [h for h in missing_hours if h in active_hours_set]
                
                if not missing_hours:
                    logger.info(f"  Nenhuma hora válida para estimar")
                    continue
                
                logger.info(f"  Horas a estimar: {missing_hours}")
                
                for hour in missing_hours:
                    try:
                        # TENTATIVA 1: Usar câmeras similares funcionando
                        estimate = self._simple_estimate_from_reference(
                            camera_id, hour, target_date, working_cameras
                        )
                        
                        # TENTATIVA 2: Se falhou, usar histórico próprio
                        if estimate == (0, 0):
                            estimate = self._estimate_from_own_history_simple(
                                camera_id, hour, target_date
                            )
                        
                        # TENTATIVA 3: Se ainda falhou, usar padrão de dia da semana
                        if estimate == (0, 0):
                            estimate = self._estimate_from_weekday_pattern_simple(
                                camera_id, hour, target_date
                            )
                        
                        # TENTATIVA 4: Fallback
                        if estimate == (0, 0):
                            estimate = self._fallback_estimate_simple(hour)
                        
                        final_inside, final_outside = estimate
                        
                        # Ajustar para garantir inside >= outside (se total > 0)
                        total = final_inside + final_outside
                        if total > 0 and final_inside < final_outside:
                            # Normalmente mais pessoas entram do que saem
                            ratio = 0.6  # 60% inside, 40% outside
                            final_inside = int(total * ratio)
                            final_outside = total - final_inside
                        
                        # Garantir não negativo
                        final_inside = max(0, final_inside)
                        final_outside = max(0, final_outside)
                        
                        # Criar registro
                        hour_timestamp = datetime(
                            target_date.year, target_date.month, target_date.day, hour
                        )
                        
                        record = {
                            'created_at': hour_timestamp,
                            'camera_id': camera_id,
                            'total_inside': final_inside,
                            'total_outside': final_outside,
                            'valid': 1,
                            'estimated': 1,
                            'client': self.cameras_df[self.cameras_df['id'] == camera_id]['client'].iloc[0],
                            'location': self.cameras_df[self.cameras_df['id'] == camera_id]['location'].iloc[0]
                        }
                        
                        estimated_records.append(record)
                        
                        logger.info(f"  Hora {hour:02d}: estimado {final_inside}/{final_outside}")
                        
                    except Exception as e:
                        logger.error(f"  Erro estimando hora {hour}: {e}")
                        continue
            
            result_df = pd.DataFrame(estimated_records)
            logger.info(f"\nTotal estimativas geradas: {len(result_df)}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Erro na estimativa de dados: {e}")
            return pd.DataFrame()
    
    def _simple_estimate_from_reference(self, camera_id: int, hour: int,
                                      target_date: datetime, working_cameras: List[int]) -> Tuple[int, int]:
        """
        Estimativa simples usando câmeras de referência.
        """
        target_weekday = target_date.weekday()
        
        for ref_camera in working_cameras:
            # Verificar se a câmera de referência está ativa nesta hora
            ref_start, ref_end = self.get_camera_active_hours(ref_camera, target_weekday)
            if ref_start is None or ref_end is None or not (ref_start <= hour <= ref_end):
                continue
            
            # Verificar se tem dados nesta hora
            ref_data = self.flow_df_valid[
                (self.flow_df_valid['camera_id'] == ref_camera) &
                (self.flow_df_valid['date'] == target_date.date()) &
                (self.flow_df_valid['hour'] == hour)
            ]
            
            if not ref_data.empty:
                ref_row = ref_data.iloc[0]
                ref_inside = ref_row['total_inside']
                ref_outside = ref_row['total_outside']
                
                # Calcular razão histórica entre as câmeras
                ratio = self._calculate_simple_ratio(camera_id, ref_camera, hour, target_weekday)
                
                if ratio > 0:
                    estimated_inside = int(ref_inside * ratio)
                    estimated_outside = int(ref_outside * ratio)
                    return (estimated_inside, estimated_outside)
        
        return (0, 0)
    
    def _calculate_simple_ratio(self, camera_a: int, camera_b: int, hour: int, weekday: int) -> float:
        """
        Calcula razão simples entre duas câmeras.
        """
        try:
            data_a = self.flow_df_valid[
                (self.flow_df_valid['camera_id'] == camera_a) &
                (self.flow_df_valid['hour'] == hour) &
                (self.flow_df_valid['weekday'] == weekday)
            ]
            
            data_b = self.flow_df_valid[
                (self.flow_df_valid['camera_id'] == camera_b) &
                (self.flow_df_valid['hour'] == hour) &
                (self.flow_df_valid['weekday'] == weekday)
            ]
            
            if data_a.empty or data_b.empty:
                return 1.0  # Razão padrão
            
            # Encontrar datas comuns
            dates_a = set(data_a['date'].unique())
            dates_b = set(data_b['date'].unique())
            common_dates = dates_a & dates_b
            
            if len(common_dates) < 2:
                return 1.0
            
            # Calcular razões
            ratios = []
            for date in common_dates:
                total_a = data_a[data_a['date'] == date]['total_traffic'].sum()
                total_b = data_b[data_b['date'] == date]['total_traffic'].sum()
                
                if total_b > 0:
                    ratio = total_a / total_b
                    # Filtrar outliers
                    if 0.1 < ratio < 10:
                        ratios.append(ratio)
            
            if ratios:
                return np.median(ratios)
            else:
                return 1.0
                
        except Exception as e:
            logger.error(f"Erro calculando razão entre {camera_a} e {camera_b}: {e}")
            return 1.0
    
    def _estimate_from_own_history_simple(self, camera_id: int, hour: int,
                                        target_date: datetime) -> Tuple[int, int]:
        """
        Estimativa simples do histórico próprio.
        """
        target_weekday = target_date.weekday()
        
        try:
            hist_data = self.flow_df_valid[
                (self.flow_df_valid['camera_id'] == camera_id) &
                (self.flow_df_valid['hour'] == hour) &
                (self.flow_df_valid['weekday'] == target_weekday) &
                (self.flow_df_valid['date'] < target_date.date())
            ]
            
            if len(hist_data) >= 2:
                # Usar mediana para robustez
                median_inside = hist_data['total_inside'].median()
                median_outside = hist_data['total_outside'].median()
                
                return (int(median_inside), int(median_outside))
            else:
                return (0, 0)
                
        except Exception as e:
            logger.error(f"Erro estimando do histórico: {e}")
            return (0, 0)
    
    def _estimate_from_weekday_pattern_simple(self, camera_id: int, hour: int,
                                            target_date: datetime) -> Tuple[int, int]:
        """
        Estimativa usando padrão de dia da semana.
        """
        try:
            # Obter média de todos os dados desta câmera nesta hora
            all_data = self.flow_df_valid[
                (self.flow_df_valid['camera_id'] == camera_id) &
                (self.flow_df_valid['hour'] == hour)
            ]
            
            if not all_data.empty:
                avg_inside = all_data['total_inside'].mean()
                avg_outside = all_data['total_outside'].mean()
                
                # Ajustar pelo fator do dia da semana
                weekday_factors = self._get_simple_weekday_factors(camera_id)
                target_weekday = target_date.weekday()
                
                if target_weekday in weekday_factors:
                    factor = weekday_factors[target_weekday]
                    avg_inside *= factor
                    avg_outside *= factor
                
                return (int(avg_inside), int(avg_outside))
            else:
                return (0, 0)
                
        except Exception as e:
            logger.error(f"Erro estimando do padrão de dia: {e}")
            return (0, 0)
    
    def _get_simple_weekday_factors(self, camera_id: int) -> Dict[int, float]:
        """
        Calcula fatores simples de dia da semana.
        """
        try:
            data = self.flow_df_valid[self.flow_df_valid['camera_id'] == camera_id]
            
            if data.empty:
                return {i: 1.0 for i in range(7)}
            
            weekday_avgs = {}
            for weekday in range(7):
                weekday_data = data[data['weekday'] == weekday]
                if not weekday_data.empty:
                    weekday_avgs[weekday] = weekday_data['total_traffic'].mean()
            
            if not weekday_avgs:
                return {i: 1.0 for i in range(7)}
            
            # Calcular média geral
            overall_avg = np.mean(list(weekday_avgs.values()))
            
            if overall_avg == 0:
                return {i: 1.0 for i in range(7)}
            
            # Calcular fatores
            factors = {wd: avg/overall_avg for wd, avg in weekday_avgs.items()}
            
            # Preencher dias faltantes
            for wd in range(7):
                if wd not in factors:
                    factors[wd] = 1.0
            
            return factors
            
        except Exception as e:
            logger.error(f"Erro calculando fatores de dia: {e}")
            return {i: 1.0 for i in range(7)}
    
    def _fallback_estimate_simple(self, hour: int) -> Tuple[int, int]:
        """
        Fallback simples baseado na hora do dia.
        """
        # Valores padrão baseados na hora
        if 6 <= hour <= 9:    # Manhã cedo
            return (15, 12)
        elif 10 <= hour <= 14:  # Meio-dia
            return (25, 20)
        elif 15 <= hour <= 18:  # Tarde
            return (35, 30)
        elif 19 <= hour <= 22:  # Noite
            return (20, 15)
        else:  # Madrugada
            return (5, 3)
    
    def insert_estimated_data(self, estimated_df: pd.DataFrame) -> Tuple[int, int]:
        """
        Insere dados estimados no banco de dados.
        """
        if estimated_df.empty:
            logger.info("Nenhum dado estimado para inserir")
            return 0, 0
        
        try:
            self.connect()
            cursor = self.conn.cursor()
            
            inserted_count = 0
            updated_count = 0
            
            logger.info(f"Inserindo {len(estimated_df)} registros estimados...")
            
            for _, row in estimated_df.iterrows():
                try:
                    # Converter timestamp
                    created_at = self._convert_timestamp(row['created_at'])
                    camera_id = int(row['camera_id'])
                    
                    # Verificar se já existe
                    cursor.execute("""
                        SELECT id FROM peopleflowtotals 
                        WHERE camera_id = ? AND created_at = ?
                    """, (camera_id, created_at))
                    
                    existing = cursor.fetchone()
                    
                    if existing is None:
                        # Inserir novo registro
                        cursor.execute("""
                            INSERT INTO peopleflowtotals 
                            (created_at, camera_id, total_inside, total_outside, valid)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            created_at,
                            camera_id,
                            int(row['total_inside']),
                            int(row['total_outside']),
                            1
                        ))
                        inserted_count += 1
                    else:
                        # Atualizar registro existente
                        cursor.execute("""
                            UPDATE peopleflowtotals 
                            SET total_inside = ?, total_outside = ?, valid = 1
                            WHERE camera_id = ? AND created_at = ?
                        """, (
                            int(row['total_inside']),
                            int(row['total_outside']),
                            camera_id,
                            created_at
                        ))
                        updated_count += 1
                        
                except Exception as e:
                    logger.error(f"Erro inserindo registro: {e}")
                    continue
            
            self.conn.commit()
            
            logger.info(f"\nResumo inserção:")
            logger.info(f"  ✅ Inseridos: {inserted_count}")
            logger.info(f"  🔄 Atualizados: {updated_count}")
            
            return inserted_count, updated_count
            
        except Exception as e:
            logger.error(f"Erro na inserção de dados: {e}")
            return 0, 0
    
    def _convert_timestamp(self, timestamp) -> str:
        """Converte timestamp para formato SQLite."""
        try:
            if isinstance(timestamp, pd.Timestamp):
                return timestamp.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(timestamp, datetime):
                return timestamp.strftime('%Y-%m-%d %H:%M:%S')
            else:
                return str(timestamp)
        except:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def process_client_location(self, client: str, location: str, 
                               days_back: int = 60) -> Dict:
        """
        Processa um par cliente-localização.
        """
        results = {
            'client': client,
            'location': location,
            'success': False,
            'error': None,
            'cameras_loaded': 0,
            'failing_cameras': 0,
            'hours_estimated': 0,
            'records_inserted': 0,
            'records_updated': 0
        }
        
        try:
            # Carregar dados
            if not self.load_data_for_client_location(client, location, days_back):
                results['error'] = "Falha ao carregar dados"
                return results
            
            results['cameras_loaded'] = len(self.cameras_df)
            
            # Obter último dia válido
            target_date = self.get_last_valid_day()
            if target_date is None:
                results['error'] = "Nenhum dado válido encontrado"
                return results
            
            # Identificar falhas
            failing_cameras = self.identify_failing_cameras(target_date)
            results['failing_cameras'] = len(failing_cameras)
            
            if not failing_cameras:
                logger.info("Nenhuma falha detectada")
                results['success'] = True
                return results
            
            # Estimar dados
            estimated_data = self.estimate_missing_data(failing_cameras, target_date)
            results['hours_estimated'] = len(estimated_data)
            
            if estimated_data.empty:
                logger.info("Nenhum dado estimado")
                results['success'] = True
                return results
            
            # Inserir dados
            inserted, updated = self.insert_estimated_data(estimated_data)
            results['records_inserted'] = inserted
            results['records_updated'] = updated
            
            results['success'] = True
            
        except Exception as e:
            results['error'] = str(e)
            logger.error(f"Erro processando {client}-{location}: {e}")
            
        finally:
            try:
                self.disconnect()
            except:
                pass
        
        return results
    
    def run_imputation(self, days_back: int = 60):
        """
        Executa imputação para todos clientes-localizações.
        """
        logger.info("=" * 60)
        logger.info("SISTEMA DE IMPUTAÇÃO DE DADOS DE CÂMERAS")
        logger.info("=" * 60)
        
        try:
            # Obter clientes-localizações
            client_locations = self.get_client_location_list()
            
            if not client_locations:
                logger.warning("Nenhum cliente-localização para processar")
                return
            
            logger.info(f"Processando {len(client_locations)} pares cliente-localização")
            
            all_results = []
            successful = 0
            
            # Processar cada par
            for i, (client, location) in enumerate(client_locations, 1):
                logger.info(f"\n{'='*60}")
                logger.info(f"Processando {i}/{len(client_locations)}: {client} - {location}")
                logger.info(f"{'='*60}")
                
                try:
                    result = self.process_client_location(client, location, days_back)
                    all_results.append(result)
                    
                    if result['success']:
                        successful += 1
                        logger.info(f"✓ Sucesso: {client} - {location}")
                    else:
                        logger.error(f"✗ Falha: {client} - {location}: {result.get('error', 'Erro desconhecido')}")
                    
                    # Resumo parcial
                    logger.info(f"Resumo para {client} - {location}:")
                    logger.info(f"  Câmeras: {result['cameras_loaded']}")
                    logger.info(f"  Falhas: {result['failing_cameras']}")
                    logger.info(f"  Horas estimadas: {result['hours_estimated']}")
                    logger.info(f"  Inseridos: {result['records_inserted']}")
                    logger.info(f"  Atualizados: {result['records_updated']}")
                    
                except Exception as e:
                    logger.error(f"Erro processando {client}-{location}: {e}")
                    all_results.append({
                        'client': client,
                        'location': location,
                        'success': False,
                        'error': str(e),
                        'cameras_loaded': 0,
                        'failing_cameras': 0,
                        'hours_estimated': 0,
                        'records_inserted': 0,
                        'records_updated': 0
                    })
            
            # Resumo final
            logger.info("\n" + "=" * 60)
            logger.info("PROCESSAMENTO CONCLUÍDO")
            logger.info("=" * 60)
            
            # Salvar relatório
            self._save_report(all_results)
            
            # Imprimir resumo
            self._print_summary(all_results, successful, len(client_locations))
            
        except Exception as e:
            logger.error(f"Erro fatal no processamento: {e}")
    
    def _save_report(self, results: List[Dict]):
        """Salva relatório do processamento."""
        try:
            df_report = pd.DataFrame(results)
            df_report.to_csv('imputation_report.csv', index=False)
            logger.info("Relatório salvo em imputation_report.csv")
        except Exception as e:
            logger.error(f"Erro salvando relatório: {e}")
    
    def _print_summary(self, results: List[Dict], successful: int, total: int):
        """Imprime resumo do processamento."""
        try:
            total_cameras = sum(r.get('cameras_loaded', 0) for r in results)
            total_failing = sum(r.get('failing_cameras', 0) for r in results)
            total_hours = sum(r.get('hours_estimated', 0) for r in results)
            total_inserted = sum(r.get('records_inserted', 0) for r in results)
            total_updated = sum(r.get('records_updated', 0) for r in results)
            
            logger.info(f"\nResumo Geral:")
            logger.info(f"  Clientes-localizações: {total}")
            logger.info(f"  Processados com sucesso: {successful}")
            logger.info(f"  Câmeras carregadas: {total_cameras}")
            logger.info(f"  Câmeras com falha: {total_failing}")
            logger.info(f"  Horas estimadas: {total_hours}")
            logger.info(f"  Registros inseridos: {total_inserted}")
            logger.info(f"  Registros atualizados: {total_updated}")
            
        except Exception as e:
            logger.error(f"Erro gerando resumo: {e}")


def main():
    """Função principal."""
    # Configuração
    DB_PATH = "nodehub.db"  # Atualize com o caminho correto
    
    # Pode especificar clientes específicos ou None para todos
    TARGET_CLIENT_LOCATIONS = [
        ('net3rcorp', 'teste')
    ]
    
    # Criar e executar imputador
    imputer = CameraDataImputer(DB_PATH, TARGET_CLIENT_LOCATIONS)
    
    try:
        imputer.run_imputation(days_back=60)
    except KeyboardInterrupt:
        logger.info("Processamento interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            imputer.disconnect()
        except:
            pass


if __name__ == "__main__":
    main()