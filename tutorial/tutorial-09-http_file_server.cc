#include <signal.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <utility>
#include <string>
#include "workflow/HttpMessage.h"
#include "workflow/WFHttpServer.h"
#include "workflow/WFTaskFactory.h"
#include "workflow/Workflow.h"

#ifndef _WIN32
#include <unistd.h>
#endif

#ifdef _WIN32
#include <WinSock2.h>
#include <Windows.h>
#else
#include <unistd.h>
#endif

using namespace protocol;

void pread_callback(WFFileIOTask *task)
{
	FileIOArgs *args = task->get_args();
	long ret = task->get_retval();
	HttpResponse *resp = (HttpResponse *)task->user_data;

#ifdef _WIN32
	CloseHandle(args->file);
#else
	close(args->fd);
#endif
	if (task->get_state() != WFT_STATE_SUCCESS || ret < 0)
	{
		resp->set_status_code("503");
		resp->append_output_body("<html>503 Internal Server Error.</html>");
	}
	else /* Use '_nocopy' carefully. */
		resp->append_output_body_nocopy(args->buf, ret);
}

void process(WFHttpTask *server_task, const char *root)
{
	HttpRequest *req = server_task->get_req();
	HttpResponse *resp = server_task->get_resp();
	const char *uri = req->get_request_uri();
	const char *p = uri;

	printf("Request-URI: %s\n", uri);
	while (*p && *p != '?')
		p++;

	std::string abs_path(uri, p - uri);
	abs_path = root + abs_path;
	if (abs_path.back() == '/')
		abs_path += "index.html";

	resp->add_header_pair("Server", "Sogou C++ Workflow Server");
#ifdef _WIN32
	HANDLE file = CreateFileA(abs_path.c_str(), GENERIC_READ,
							  FILE_SHARE_READ, NULL, OPEN_EXISTING,
							  FILE_ATTRIBUTE_NORMAL, NULL);
	if (file != INVALID_HANDLE_VALUE)
	{
		LARGE_INTEGER sz;
		GetFileSizeEx(file, &sz);
		size_t size = (size_t)sz.QuadPart;
		void *buf = malloc(size ? size : 1);
		WFFileIOTask *pread_task;

		pread_task = WFTaskFactory::create_pread_task(file, buf, size, 0,
													  pread_callback);
		pread_task->user_data = resp;
		server_task->user_data = buf;
		server_task->set_callback([](WFHttpTask *t){ free(t->user_data); });
		series_of(server_task)->push_back(pread_task);
	}
	else
#else
	int fd = open(abs_path.c_str(), O_RDONLY);
	if (fd >= 0)
	{
		size_t size = lseek(fd, 0, SEEK_END);
		lseek(fd, 0, SEEK_SET);
		void *buf = malloc(size ? size : 1);
		WFFileIOTask *pread_task;

		pread_task = WFTaskFactory::create_pread_task(fd, buf, size, 0,
													  pread_callback);
		pread_task->user_data = resp;
		server_task->user_data = buf;
		server_task->set_callback([](WFHttpTask *t){ free(t->user_data); });
		series_of(server_task)->push_back(pread_task);
	}
	else
#endif
	{
		resp->set_status_code("404");
		resp->append_output_body("<html>404 Not Found.</html>");
	}
}
void sig_handler(int signo) { }

int main(int argc, char *argv[])
{
	if (argc != 2 && argc != 3 && argc != 5)
	{
		fprintf(stderr, "%s <port> [root path] [cert file] [key file]\n",
				argv[0]);
		exit(1);
	}

	signal(SIGINT, sig_handler);

	unsigned short port = atoi(argv[1]);
	const char *root = (argc >= 3 ? argv[2] : ".");
	auto&& proc = std::bind(process, std::placeholders::_1, root);
	WFHttpServer server(proc);
	int ret;

	if (argc == 5)
		ret = server.start(port, argv[3], argv[4]);	/* https server */
	else
		ret = server.start(port);

	if (ret == 0)
	{
#ifndef _WIN32
		pause();
#else
		getchar();
#endif
		server.stop();
	}
	else
	{
		perror("start server");
		exit(1);
	}

	return 0;
}

