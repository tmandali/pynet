import time
import logging
from functools import wraps
from typing import Callable, Any, Iterator

logger = logging.getLogger(__name__)

def watch_time_decorator(func: Callable) -> Callable:
    """
    Fonksiyonun veya generator'ın çalışma süresini ölçen decorator.
    Generator'lar için her iterasyon arasında da bilgi verir.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        
        # Generator kontrolü
        if hasattr(result, '__iter__') and not isinstance(result, (str, bytes)):
            iteration_count = 0
            last_iter_time = start_time
            
            def timed_generator():
                nonlocal iteration_count, last_iter_time
                try:
                    for item in result:
                        iteration_count += 1
                        current_time = time.time()
                        iter_elapsed = current_time - last_iter_time
                        total_elapsed = current_time - start_time
                        
                        logger.info(
                            f"{func.__name__} - Iterasyon {iteration_count}: "
                            f"Bu iterasyon: {iter_elapsed:.2f}s, Toplam: {total_elapsed:.2f}s"
                        )
                        last_iter_time = current_time
                        yield item
                    
                    final_elapsed = time.time() - start_time
                    logger.info(
                        f"{func.__name__} tamamlandı - "
                        f"Toplam iterasyon: {iteration_count}, Toplam süre: {final_elapsed:.2f} saniye"
                    )
                except Exception as e:
                    elapsed = time.time() - start_time
                    logger.error(
                        f"{func.__name__} hata ile sonlandı - "
                        f"Süre: {elapsed:.2f} saniye, Iterasyon: {iteration_count} - Hata: {e}"
                    )
                    raise
            
            return timed_generator()
        else:
            # Normal fonksiyon
            elapsed = time.time() - start_time
            logger.info(f"{func.__name__} tamamlandı - Süre: {elapsed:.2f} saniye")
            return result
    
    return wrapper