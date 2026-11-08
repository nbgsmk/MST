package cc.kostic.mst;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WorkerController {

	private final WorkerService workerService;

	public WorkerController(WorkerService workerService) {
		this.workerService = workerService;
	}

	@GetMapping("/status")
	public WorkerService.WorkerStatus status() {
		return workerService.status();
	}

	@PostMapping("/start")
	public ResponseEntity<WorkerService.WorkerStatus> start() {
		workerService.start();
		return ResponseEntity.ok(workerService.status());
	}

	@PostMapping("/stop")
	public ResponseEntity<WorkerService.WorkerStatus> stop() {
		workerService.stop();
		return ResponseEntity.ok(workerService.status());
	}

	@PostMapping("/restart")
	public ResponseEntity<WorkerService.WorkerStatus> restart() {
		workerService.restart();
		return ResponseEntity.ok(workerService.status());
	}
}
