import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeApiError,
	BinaryData, // Import BinaryData
} from 'n8n-workflow';
import Jimp from 'jimp'; // Use default import for Jimp v0.16+
import * as path from 'path'; // Import path for extension checking

// Helper function to get MIME type from extension
function getMimeType(extension: string): string {
	const ext = extension.toLowerCase().startsWith('.') ? extension.toLowerCase() : `.${extension.toLowerCase()}`;
	const mimeTypes: Record<string, string> = {
		'.jpg': 'image/jpeg',
		'.jpeg': 'image/jpeg',
		'.png': 'image/png',
		'.gif': 'image/gif', // Often handled as video/animation
		'.webp': 'image/webp',
		'.mp4': 'video/mp4',
		'.webm': 'video/webm',
		'.mov': 'video/quicktime',
		'.avi': 'video/x-msvideo',
		// Add more mappings as needed
	};
	return mimeTypes[ext] || 'application/octet-stream'; // Default fallback
}

export class Comfyui implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'ComfyUI',
		name: 'comfyui',
		icon: 'file:comfyui.svg',
		group: ['transform'],
		version: 1.1, // Increment version for changes
		description: 'Execute ComfyUI workflows and retrieve image/video outputs',
		defaults: {
			name: 'ComfyUI',
		},
		credentials: [
			{
				name: 'comfyUIApi',
				required: true,
			},
		],
		inputs: ['main'],
		outputs: ['main'],
		properties: [
			{
				displayName: 'Workflow JSON',
				name: 'workflow',
				type: 'string',
				typeOptions: {
					rows: 10,
				},
				default: '',
				required: true,
				description: 'The ComfyUI workflow in JSON format',
			},
			{
				displayName: 'Image Output Format', // Renamed for clarity
				name: 'imageOutputFormat', // Renamed
				type: 'options',
				options: [
					{
						name: 'JPEG',
						value: 'jpeg',
					},
					{
						name: 'PNG',
						value: 'png',
					},
					{
						name: 'Original (if image)', // Option to keep original image format
						value: 'original',
					},
				],
				default: 'jpeg',
				description: 'The format for output *images*. Videos retain their original format.',
			},
			{
				displayName: 'Image JPEG Quality', // Renamed for clarity
				name: 'imageJpegQuality', // Renamed
				type: 'number',
				typeOptions: {
					minValue: 1,
					maxValue: 100
				},
				default: 80,
				description: 'Quality of JPEG image output (1-100)',
				displayOptions: {
					show: {
						imageOutputFormat: ['jpeg'], // Adjusted display option
					},
				},
			},
			{
				displayName: 'Timeout',
				name: 'timeout',
				type: 'number',
				default: 30,
				description: 'Maximum time in minutes to wait for workflow completion',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const credentials = await this.getCredentials('comfyUIApi');
		const workflow = this.getNodeParameter('workflow', 0) as string;
		const timeout = this.getNodeParameter('timeout', 0) as number;
		const imageOutputFormat = this.getNodeParameter('imageOutputFormat', 0) as string; // Renamed variable
		let imageJpegQuality = 80; // Default value
		if (imageOutputFormat === 'jpeg') {
			imageJpegQuality = this.getNodeParameter('imageJpegQuality', 0) as number; // Renamed variable
		}

		const apiUrl = credentials.apiUrl as string;
		const apiKey = credentials.apiKey as string;

		console.log('[ComfyUI] Executing with API URL:', apiUrl);

		const headers: Record<string, string> = {
			'Content-Type': 'application/json',
		};

		if (apiKey) {
			console.log('[ComfyUI] Using API key authentication');
			headers['Authorization'] = `Bearer ${apiKey}`;
		}

		try {
			// Check API connection
			console.log('[ComfyUI] Checking API connection...');
			await this.helpers.request({
				method: 'GET',
				url: `${apiUrl}/system_stats`,
				headers,
				json: true,
			});
			console.log('[ComfyUI] API connection successful.');

			// Queue prompt
			console.log('[ComfyUI] Queueing prompt...');
			let parsedWorkflow;
			try {
				parsedWorkflow = JSON.parse(workflow);
			} catch (e) {
				throw new NodeApiError(this.getNode(), { message: `Invalid Workflow JSON: ${e.message}` });
			}

			const response = await this.helpers.request({
				method: 'POST',
				url: `${apiUrl}/prompt`,
				headers,
				body: {
					prompt: parsedWorkflow,
				},
				json: true,
			});

			if (!response || !response.prompt_id) {
				console.error('[ComfyUI] Failed prompt response:', response);
				throw new NodeApiError(this.getNode(), { message: 'Failed to get prompt ID from ComfyUI. Check ComfyUI logs.' });
			}

			const promptId = response.prompt_id;
			console.log('[ComfyUI] Prompt queued with ID:', promptId);

			// --- Polling Logic ---
			const startTime = Date.now();
			const timeoutMillis = timeout * 60 * 1000;
			let executionFinished = false;
			let lastStatus = '';

			while (Date.now() - startTime < timeoutMillis) {
				await new Promise(resolve => setTimeout(resolve, 2000)); // Poll every 2 seconds

				let history: any;
				try {
					history = await this.helpers.request({
						method: 'GET',
						url: `${apiUrl}/history/${promptId}`,
						headers,
						json: true,
						// Allow non-2xx responses during polling as history might not exist yet
						validateStatus: (status) => status >= 200 && status < 500,
					});

					// Check if history for the prompt exists
					if (!history || !history[promptId]) {
						// It's possible the prompt is still queued and not yet in history
						// Let's check the queue status as well
						const queueData = await this.helpers.request({
							method: 'GET',
							url: `${apiUrl}/queue`,
							headers,
							json: true,
						});
						const isInQueueRunning = queueData?.queue_running?.some((item: any[]) => item[1] === promptId);
						const isInQueuePending = queueData?.queue_pending?.some((item: any[]) => item[1] === promptId);

						if (isInQueueRunning) {
							if (lastStatus !== 'running') {
								console.log(`[ComfyUI] Prompt ${promptId} is running...`);
								lastStatus = 'running';
							}
						} else if (isInQueuePending) {
							if (lastStatus !== 'pending') {
								console.log(`[ComfyUI] Prompt ${promptId} is pending in queue...`);
								lastStatus = 'pending';
							}
						} else {
							if (lastStatus !== 'waiting') {
								console.log(`[ComfyUI] Waiting for prompt ${promptId} to appear in history...`);
								lastStatus = 'waiting';
							}
						}
						continue; // Continue polling
					}

				} catch (error) {
					console.error(`[ComfyUI] Error polling history for prompt ${promptId}:`, error);
					// Decide if the error is fatal or recoverable (e.g., temporary network issue)
					// For now, continue polling, but could add retry limits or specific error handling
					await new Promise(resolve => setTimeout(resolve, 5000)); // Wait longer after an error
					continue;
				}


				const promptResult = history[promptId];

				// ComfyUI history might show up but without status initially
				const status = promptResult.status;
				if (!status && lastStatus !== 'processing') {
					console.log(`[ComfyUI] Prompt ${promptId} found in history, waiting for status...`);
					lastStatus = 'processing';
					continue;
				}

				if (status?.status_str === 'error' || status?.error) {
					console.error(`[ComfyUI] Workflow execution failed for prompt ${promptId}. Status:`, status);
					// Attempt to get more error details if available
					let errorMessage = '[ComfyUI] Workflow execution failed.';
					if (status.messages) {
						errorMessage += ` Messages: ${JSON.stringify(status.messages)}`;
					}
					// Look for errors in node outputs as well
					if (promptResult.outputs) {
						for (const nodeId in promptResult.outputs) {
							const nodeOutput = promptResult.outputs[nodeId];
							if (nodeOutput.errors) {
								errorMessage += ` Node ${nodeId} errors: ${JSON.stringify(nodeOutput.errors)}`;
							}
						}
					}
					throw new NodeApiError(this.getNode(), { message: errorMessage }, { itemIndex: 0 });
				}


				if (status?.completed) {
					console.log(`[ComfyUI] Execution completed for prompt ${promptId}`);
					executionFinished = true; // Mark as finished to exit loop

					// --- Process Outputs ---
					if (!promptResult.outputs) {
						console.warn(`[ComfyUI] No outputs found in workflow result for prompt ${promptId}.`);
						return [[]]; // Return empty result if no outputs
					}

					const outputProcessingPromises: Promise<INodeExecutionData | null>[] = [];

					// Iterate through all nodes in the output
					for (const nodeId in promptResult.outputs) {
						const nodeOutput = promptResult.outputs[nodeId];
						// Iterate through all keys in that node's output (e.g., 'images', 'gifs', 'videos')
						for (const outputKey in nodeOutput) {
							const files = nodeOutput[outputKey];
							// Check if it's an array (common pattern for file lists)
							if (Array.isArray(files)) {
								files.forEach((file: any) => {
									// Check if the item looks like a ComfyUI file output object
									if (file && typeof file === 'object' && file.filename && file.type) {
										console.log(`[ComfyUI] Found potential output file: ${file.filename} (type: ${file.type}, key: ${outputKey})`);
										// Add a promise to process this file
										outputProcessingPromises.push(
											this.processOutputFile(file, apiUrl, headers, imageOutputFormat, imageJpegQuality)
												.catch(error => {
													// Log errors during individual file processing but don't stop others
													console.error(`[ComfyUI] Failed to process file ${file.filename}:`, error);
													return { // Return an error structure for this item
														json: {
															filename: file.filename,
															type: file.type,
															subfolder: file.subfolder || '',
															error: `Failed to process: ${error.message}`,
														},
													};
												})
										);
									}
								});
							}
						}
					}

					const processedOutputs = (await Promise.all(outputProcessingPromises))
											 .filter(output => output !== null) as INodeExecutionData[]; // Filter out nulls (e.g., unsupported types)

					console.log(`[ComfyUI] Successfully processed ${processedOutputs.length} output files.`);
					return [processedOutputs];
				} else {
					// Log progress if available
					if (status?.status_str && status.status_str !== lastStatus) {
						lastStatus = status.status_str;
						const progress = status.progress ? ` (${(status.progress * 100).toFixed(1)}%)` : '';
						console.log(`[ComfyUI] Execution status for ${promptId}: ${lastStatus}${progress}`);
					}
				}
			} // End of while loop

			// If loop finishes without executionFinished being true, it's a timeout
			if (!executionFinished) {
				throw new NodeApiError(this.getNode(), { message: `Execution timeout after ${timeout} minutes for prompt ${promptId}` });
			}

			// This part should technically be unreachable if logic is correct, but as a safeguard:
			return [[]];

		} catch (error) {
			console.error('[ComfyUI] Execution error:', error);
			// Handle potential NodeApiError specifically if needed, otherwise wrap
			if (error instanceof NodeApiError) {
				throw error; // Re-throw NodeApiError as is
			}
			// Ensure message is extracted correctly from potential nested errors
			const message = error.message || (error.cause as Error)?.message || JSON.stringify(error);
			throw new NodeApiError(this.getNode(), { message: `ComfyUI API Error: ${message}` }, { cause: error });
		}
	}

	/**
	 * Downloads and processes a single output file (image or video).
	 * Returns INodeExecutionData or null if the file type is unsupported/unhandled.
	 */
	async processOutputFile(
		this: IExecuteFunctions,
		file: { filename: string; subfolder?: string; type?: string },
		apiUrl: string,
		headers: Record<string, string>,
		imageOutputFormat: string,
		imageJpegQuality: number,
	): Promise<INodeExecutionData | null> {

		const filename = file.filename;
		const fileExtension = path.extname(filename).toLowerCase();
		const mimeType = getMimeType(fileExtension);
		const isImage = mimeType.startsWith('image/');
		const isVideo = mimeType.startsWith('video/') || mimeType === 'image/gif'; // Treat animated GIF as video type

		console.log(`[ComfyUI] Processing file: ${filename}, Extension: ${fileExtension}, MIME: ${mimeType}, IsImage: ${isImage}, IsVideo: ${isVideo}`);

		if (!isImage && !isVideo) {
			console.warn(`[ComfyUI] Skipping unsupported file type: ${filename} (MIME: ${mimeType})`);
			return null; // Skip unsupported file types
		}

		const downloadUrl = `${apiUrl}/view?filename=${encodeURIComponent(filename)}&subfolder=${encodeURIComponent(file.subfolder || '')}&type=${encodeURIComponent(file.type || 'output')}`;
		console.log(`[ComfyUI] Downloading from: ${downloadUrl}`);

		let fileDataBuffer: Buffer;
		try {
			// Download as raw buffer
			const responseData = await this.helpers.request({
				method: 'GET',
				url: downloadUrl,
				headers,
				encoding: null, // Get raw buffer
				validateStatus: (status) => status >= 200 && status < 300, // Ensure successful download
			}) as Buffer; // Cast the result to Buffer explicitly
			fileDataBuffer = responseData;
			console.log(`[ComfyUI] Downloaded ${filename} (${(fileDataBuffer.length / 1024).toFixed(2)} kB)`);

		} catch (error) {
			console.error(`[ComfyUI] Failed to download file ${filename} from ${downloadUrl}:`, error);
			// Rethrow or return an error structure that will be caught by the caller
			throw new Error(`Download failed for ${filename}: ${error.message}`);
		}

		let finalBuffer: Buffer;
		let finalMimeType = mimeType;
		let finalFileExtension = fileExtension.substring(1); // Remove leading dot
		let finalFileType: 'image' | 'video' = isVideo ? 'video' : 'image'; // Default to image if not video

		if (isImage && imageOutputFormat !== 'original') {
			// Process image with Jimp for format conversion/quality
			try {
				const image = await Jimp.read(fileDataBuffer);
				if (imageOutputFormat === 'jpeg') {
					finalMimeType = 'image/jpeg';
					finalFileExtension = 'jpeg';
					finalBuffer = await image.quality(imageJpegQuality).getBufferAsync(Jimp.MIME_JPEG);
				} else { // PNG
					finalMimeType = 'image/png';
					finalFileExtension = 'png';
					finalBuffer = await image.getBufferAsync(Jimp.MIME_PNG);
				}
				console.log(`[ComfyUI] Converted image ${filename} to ${finalMimeType}`);
			} catch (jimpError) {
				console.error(`[ComfyUI] Jimp failed to process image ${filename}. Falling back to original data. Error:`, jimpError);
				// Fallback to using original data if Jimp fails
				finalBuffer = fileDataBuffer;
				finalMimeType = mimeType; // Keep original mime type
				finalFileExtension = fileExtension.substring(1); // Keep original extension
				finalFileType = 'image'; // Still mark as image
			}
		} else {
			// Use original data for videos or if 'original' format is selected for images
			finalBuffer = fileDataBuffer;
			// Ensure file type is correctly set for videos/gifs
			if (isVideo) finalFileType = 'video';
		}

		// Create Binary Data
		const binaryData = await this.helpers.prepareBinaryData(finalBuffer, filename, finalMimeType);

		// Assemble execution data
		const item: INodeExecutionData = {
			json: {
				filename: filename,
				originalType: file.type,
				subfolder: file.subfolder || '',
				outputMimeType: finalMimeType,
				outputExtension: finalFileExtension,
				outputFileType: finalFileType,
				sizeKB: Math.round(finalBuffer.length / 1024 * 10) / 10,
			},
			binary: {
				// Use the key 'data' which n8n expects for the main binary output
				data: binaryData as BinaryData,
			}
		};

		return item;
	}
}
